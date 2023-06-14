from flask import Flask, request
from time import sleep

app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def handle():
    sleep(5)
    print(request.json)
    return "It's ok!\n"


app.run()
