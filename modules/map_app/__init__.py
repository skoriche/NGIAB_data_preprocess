from flask import Flask
import logging
from map_app.views import main, intra_module_db
from data_sources.source_validation import validate_all

with open("app.log", "w") as f:
    f.write("")
    f.write("Starting Application!\n")

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

validate_all()

app = Flask(__name__)
app.register_blueprint(main)

intra_module_db["app"] = app
