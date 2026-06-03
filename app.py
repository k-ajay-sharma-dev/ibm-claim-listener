import threading
import requests
import json
from flask import Flask, jsonify, request
from confluent_kafka import Consumer, Producer

app = Flask(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKERS  = "broker-0-p8rt5300ryr2w5t2.kafka.svc07.us-south.eventstreams.cloud.ibm.com:9093,broker-1-p8rt5300ryr2w5t2.kafka.svc07.us-south.eventstreams.cloud.ibm.com:9093,broker-2-p8rt5300ryr2w5t2.kafka.svc07.us-south.eventstreams.cloud.ibm.com:9093"
KAFKA_TOPIC    = "CLAIM_INBOUND"
KAFKA_USERNAME = "token"
KAFKA_PASSWORD = "AaeIAaRelAxCPJ4NOU3rHVtHOPGRLpar15Axr_3uH-1v"

WXO_BASE_URL   = "https://api.us-south.watson-orchestrate.cloud.ibm.com/instances/091ba2cc-c814-4bd4-9922-16b2beb7fd6a"
IBM_API_KEY    = "jq5hJemMAGEdWm2_q-pkD83WKQ4WRBP3xdlgi9c94CqY"
AGENT_ID       = "26c75a07-cc8b-443b-86ea-ce7df4d68b89"

KAFKA_CONF = {
    'bootstrap.servers': KAFKA_BROKERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': KAFKA_USERNAME,
    'sasl.password': KAFKA_PASSWORD,
}

# ── IAM Token ─────────────────────────────────────────────────────────────────
def get_iam_token():
    resp = requests.post(
        "https://iam.cloud.ibm.com/identity/token",
        data={
            "grant_type": "urn:ibm:params:oauth:grant-type:apikey",
            "apikey": IBM_API_KEY
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"}
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

# ── Call WXO Agent ────────────────────────────────────────────────────────────
def invoke_orchestrate(kafka_message: dict):
    try:
        token = get_iam_token()

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        payload = {
            "agent_id": AGENT_ID,
            "message": {
                "role": "user",
                "content": json.dumps(kafka_message)
            }
        }

        resp = requests.post(
            f"{WXO_BASE_URL}/v1/orchestrate/runs",
            json=payload,
            headers=headers
        )

        print(f"[WXO] Status: {resp.status_code}")
        result = resp.json()
        print(f"[WXO] Thread ID: {result.get('thread_id')}")
        print(f"[WXO] Run ID:    {result.get('run_id')}")

    except Exception as e:
        print(f"[WXO] Error: {e}")

# ── POST Publish to Kafka ─────────────────────────────────────────────────────
@app.route("/publish", methods=["POST"])
def publish_message():
    try:
        body = request.get_json()

        if not body:
            return jsonify({"error": "Request body is required"}), 400

        topic = body.get("topic")
        message = body.get("message")

        if not topic:
            return jsonify({"error": "topic is required"}), 400
        if not message:
            return jsonify({"error": "message is required"}), 400

        delivery_results = []

        def delivery_report(err, msg):
            if err:
                delivery_results.append({"status": "failed", "error": str(err)})
            else:
                delivery_results.append({
                    "status": "delivered",
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset()
                })

        producer = Producer(KAFKA_CONF)
        producer.produce(
            topic,
            value=json.dumps(message),
            callback=delivery_report
        )
        producer.flush()

        return jsonify({
            "success": True,
            "topic": topic,
            "delivery": delivery_results[0] if delivery_results else {}
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── GET Agent Response API ────────────────────────────────────────────────────
@app.route("/response/<thread_id>", methods=["GET"])
def get_agent_response(thread_id):
    try:
        token = get_iam_token()

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        r = requests.get(
            f"{WXO_BASE_URL}/v1/orchestrate/threads/{thread_id}/messages",
            headers=headers
        )

        if r.status_code != 200:
            return jsonify({"error": "Failed to fetch messages", "status": r.status_code}), 500

        messages = r.json()
        response_texts = []

        for msg in messages:
            if msg.get("role") == "assistant":
                for block in msg.get("content", []):
                    if block.get("response_type") == "text":
                        response_texts.append(block.get("text"))

        return jsonify({
            "thread_id": thread_id,
            "responses": response_texts
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Proxy: Get IAM token + fetch threads ─────────────────────────────────────
@app.route("/threads", methods=["GET"])
def get_threads():
    try:
        token = get_iam_token()
        headers = {"Authorization": f"Bearer {token}"}

        r = requests.get(f"{WXO_BASE_URL}/v1/orchestrate/threads", headers=headers)
        threads = r.json()

        thread_data = []
        for t in threads:
            tid = t.get("id")
            mr = requests.get(
                f"{WXO_BASE_URL}/v1/orchestrate/threads/{tid}/messages",
                headers=headers
            )
            messages = mr.json()
            latest_time = messages[-1].get("created_on", "1970") if messages else "1970"
            thread_data.append({
                "thread_id": tid,
                "messages": messages,
                "latest_time": latest_time
            })

        thread_data.sort(key=lambda x: x["latest_time"], reverse=True)

        return jsonify(thread_data[:10]), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Kafka Listener ────────────────────────────────────────────────────────────
def kafka_listener():
    print("[Kafka] Starting consumer...")

    consumer = Consumer({**KAFKA_CONF, 'group.id': 'claim-listener-group', 'auto.offset.reset': 'latest'})
    consumer.subscribe([KAFKA_TOPIC])
    print("[Kafka] Listening on topic:", KAFKA_TOPIC)

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[Kafka] Error: {msg.error()}")
            continue

        raw = msg.value().decode("utf-8")
        print(f"[Kafka] Message received: {raw}")

        try:
            claim = json.loads(raw)
        except Exception:
            claim = {"raw": raw}

        invoke_orchestrate(claim)

# ── Flask Health Check ────────────────────────────────────────────────────────
@app.route("/health")
def health():
    return jsonify({"status": "running"}), 200

@app.route("/")
def index():
    endpoints = {}
    for rule in app.url_map.iter_rules():
        if rule.endpoint != 'static':
            methods = sorted([m for m in rule.methods if m not in ('HEAD', 'OPTIONS')])
            endpoints[rule.rule] = methods

    return jsonify({
        "service": "Claim Processing Listener",
        "status": "running",
        "endpoints": endpoints
    }), 200

# ── Start ─────────────────────────────────────────────────────────────────────
thread = threading.Thread(target=kafka_listener, daemon=True)
thread.start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)