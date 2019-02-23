from flask import Flask, request

import emails

app = Flask(__name__)


@app.route('/', methods=['GET'])
def hello_world():
    return 'Hello, World!'


@app.route('/', methods=['POST', 'GET'])
def run():
    print ("get json: ", request.get_json())
    to = request.get_json()["to"]
    content = request.get_json()["content"]
    print (content, to)
    emails.sendEmail(to, content)
    return '', 204


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
