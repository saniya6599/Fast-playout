from CommonServices.Global_context import GlobalContext
from flask import Flask, request, jsonify
import requests
from CommonServices.Logger import Logger

app = Flask(__name__)

class instant_restart :
    def __init__(self):
        self.global_context = GlobalContext()
        self.listener_port = self.global_context.get_value("listener_port")
        self.logger = Logger()
        
        
    def start(self):
        try:
            payload =  {
                "StartServer": False
            }
            response = requests.post(f"http://localhost:{self.listener_port+1000}/api/startserver",json=payload)
            if response.status_code == 200:
                self.logger.info("Server restarted successfully")
        except requests.exceptions.ConnectionError:
            print("Flask server not responding yet, retrying...")
