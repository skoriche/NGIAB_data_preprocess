## This file is run when the python -m map_app command is run
## It is the entry point for the application and is equivalent to run.sh
from data_processing.file_paths import file_paths
import os
import webbrowser
from threading import Timer

with open("app.log", "w") as f:
    f.write("")
    f.write("Starting Application!\n")

def open_browser():
    webbrowser.open_new("http://127.0.0.1:5000")

if file_paths.dev_file().is_file():
    with open("app.log", "a") as f:
        f.write("Running in debug mode\n")
        Timer(1, open_browser).start()
        os.system("flask -A map_app run --debug")
else:
    with open("app.log", "a") as f:
        f.write("Running in production mode\n")
        Timer(1, open_browser).start()
        os.system("flask -A map_app run")
