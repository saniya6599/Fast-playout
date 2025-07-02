import os
import json
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class FileWatcher:
    def __init__(self, directory_to_watch, json_file):
        self.directory_to_watch = directory_to_watch
        self.json_file = json_file

    def run(self):
        event_handler = Handler(self.json_file, self.directory_to_watch)
        observer = Observer()
        observer.schedule(event_handler, self.directory_to_watch, recursive=True)
        observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()

class Handler(FileSystemEventHandler):
    def __init__(self, json_file, directory_to_watch):
        self.json_file = json_file
        self.directory_to_watch = directory_to_watch
        self.update_json_file()

    def update_json_file(self):
        files = [f for f in os.listdir(self.directory_to_watch) if os.path.isfile(os.path.join(self.directory_to_watch, f))]
        with open(self.json_file, 'w') as f:
            json.dump(files, f, indent=4)

    def on_modified(self, event):
        self.update_json_file()

    def on_created(self, event):
        self.update_json_file()

    def on_deleted(self, event):
        self.update_json_file()
