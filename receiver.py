from flask import Flask
from time import sleep

app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def handle():
    sleep(300)
    return "It's ok!\n"


app.run()
