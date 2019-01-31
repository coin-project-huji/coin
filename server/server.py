import json
import emails
from flask import Flask, request

app = Flask(__name__)


@app.route('/', methods=['GET'])
def hello_world():
    return 'Hello, World!'


@app.route('/', methods=['POST', 'GET'])
def run():
    to = request.get_json()["to"]
    content = request.get_json()["content"]
    response = emails.sendEmail(to, content)
    print(response)
    data = json.dumps({
        "to": to,
        "content": content
    })
    return data


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
