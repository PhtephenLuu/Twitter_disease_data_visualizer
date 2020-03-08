from flask import Flask, jsonify, request, Response, render_template
from pykafka import KafkaClient
import json


def get_kafka_client():
    return KafkaClient(hosts='192.168.2.213:9092')


app = Flask(__name__)


@app.route("/")
def index():
    return(render_template(('index.html')))

@app.route("/home")
def home():
    return("Home")

@app.route("/topics/<topicname>")
def get_message(topicname):
    client = get_kafka_client()
    def events():
        for i in client.topics[topicname].get_simple_consumer():
            yield 'data{0}\n\n'.format(i.value.decode())
    return Response(events(), mimetype="text/event-stream")


if __name__ == '__main__':
    app.run(debug=True, port=5001)
