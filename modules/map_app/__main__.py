## This file is run when the python -m map_app command is run
## It is the entry point for the application and is equivalent to run.sh
from data_processing.file_paths import file_paths
import os
from threading import Timer
from flask import Flask
from flask_cors import CORS
from flaskwebgui import FlaskUI
import logging
from .views import intra_module_db, main

with open("app.log", "w") as f:
    f.write("")
    f.write("Starting Application!\n")

logging.getLogger("werkzeug").setLevel(logging.WARNING)

app = Flask(__name__)
app.register_blueprint(main)

intra_module_db["app"] = app

CORS(app)

logging.basicConfig(
    level=logging.INFO,
    format="%(name)-12s: %(levelname)s - %(message)s",
    filename="app.log",
    filemode="a",
)  # Append mode
# Example: Adding a console handler to root logger (optional)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # Or any other level
formatter = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
console_handler.setFormatter(formatter)
logging.getLogger("").addHandler(console_handler)

def main():
    if file_paths.dev_file().is_file():
        with open("app.log", "a") as f:
            f.write("Running in debug mode\n")
        FlaskUI(app=app, server="flask").run()
    else:
        with open("app.log", "a") as f:
            f.write("Running in production mode\n")
        FlaskUI(app=app, server="flask").run()

if __name__ == "__main__":
    main()