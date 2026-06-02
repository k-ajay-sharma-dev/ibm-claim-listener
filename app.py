from flask import Flask, request
import requests

app = Flask(__name__)

@app.route("/", methods=["POST"])
def receive():

    event = request.get_json()

    print("Received Event:")
    print(event)

    # TODO:
    # call watsonx orchestrate flow/agent here

    return {
        "status": "accepted"
    }, 200


@app.route("/health")
def health():
    return "OK"


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=8080
    )