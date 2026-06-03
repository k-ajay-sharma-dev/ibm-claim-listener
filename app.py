import threading
import requests
from flask import Flask
from kafka import KafkaConsumer

app = Flask(__name__)

# ── Config ──────────────────────────────────────────────────────────────────
KAFKA_BROKERS = [
    "broker-0-p8rt5300ryr2w5t2.kafka.svc07.us-south.eventstreams.cloud.ibm.com:9093",
    "broker-1-p8rt5300ryr2w5t2.kafka.svc07.us-south.eventstreams.cloud.ibm.com:9093",
    "broker-2-p8rt5300ryr2w5t2.kafka.svc07.us-south.eventstreams.cloud.ibm.com:9093",
    "broker-3-p8rt5300ryr2w5t2.kafka.svc07.us-south.eventstreams.cloud.ibm.com:9093",
    "broker-4-p8rt5300ryr2w5t2.kafka.svc07.us-south.eventstreams.cloud.ibm.com:9093",
    "broker-5-p8rt5300ryr2w5t2.kafka.svc07.us-south.eventstreams.cloud.ibm.com:9093",
]
KAFKA_TOPIC    = "CLAIM_INBOUND"
KAFKA_USERNAME = "token"
KAFKA_PASSWORD = "AaeIAaRelAxCPJ4NOU3rHVtHOPGRLpar15Axr_3uH-1v"

WXO_BASE_URL  = "https://api.us-south.watson-orchestrate.cloud.ibm.com/instances/091ba2cc-c814-4bd4-9922-16b2beb7fd6a"
IBM_API_KEY   = "jq5hJemMAGEdWm2_q-pkD83WKQ4WRBP3xdlgi9c94CqY"
FLOW_NAME     = "Agentic_workflow_7879lC"

# ── IAM Token ────────────────────────────────────────────────────────────────
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

# ── Call WXO Flow ─────────────────────────────────────────────────────────────
def invoke_orchestrate():
    try:
        token = get_iam_token()

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        payload = {
            "input": {
                "text": "Process incoming claim"
            }
        }

        resp = requests.post(
            f"{WXO_BASE_URL}/v1/flows/{FLOW_NAME}/run",
            json=payload,
            headers=headers
        )

        print(f"[WXO] Status: {resp.status_code}")
        print(f"[WXO] Response: {resp.text}")

    except Exception as e:
        print(f"[WXO] Error: {e}")

# ── Kafka Listener ────────────────────────────────────────────────────────────
def kafka_listener():
    print("[Kafka] Starting consumer...")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
        auto_offset_reset="latest",
        group_id="claim-listener-group"
    )

    print("[Kafka] Listening on topic:", KAFKA_TOPIC)

    for message in consumer:
        print(f"[Kafka] Message received: {message.value}")
        invoke_orchestrate()

# ── Flask Health Check ────────────────────────────────────────────────────────
@app.route("/health")
def health():
    return {"status": "running"}, 200

# ── Start ─────────────────────────────────────────────────────────────────────
thread = threading.Thread(target=kafka_listener, daemon=True)
thread.start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)