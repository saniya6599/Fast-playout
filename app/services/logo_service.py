import json
import os
import re
import time
import pandas as pd
import requests
from CommonServices.Global_context import GlobalContext
from CommonServices.DataFrameManager import DataFrameManager
from CommonServices.Logger import Logger


class logo_rendering_api:
    
    def __init__(self,base_url):
        self.global_context = GlobalContext()
        self.df_manager = DataFrameManager()
        self.base_url = base_url
        self.logger=Logger()    
        self.success=False

    def overlay(self,action):
        try:
            print(f"[INFO] Preparing to trigger logo rendering endpoint")
            self.logger.info("Preparing to trigger logo rendering endpoint")
            url = f"{self.base_url}/handle_overlays_api"
            payload = {
                "action": action,
                "event_id": f'{self.global_context.get_value("channel_id")}'
            }
            
            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Basic Zm9yYmVzOmZvcmJlc0BhcGk='
                }
            
            print(f'[INFO] URL: {url}')
            print(f'[INFO] Payload: {payload}')
            

            self.logger.info(f"overlay logo service with payload : {payload}")
            print(f"[INFO] Sending POST request to logo rendering service...")
            self.logger.info(" Sending POST request to logo rendering service...")
            response = requests.request("POST", url, data=json.dumps(payload), headers=headers)

            print(f"[INFO] Received response: {response.status_code}")
            self.logger.info(f"Received response: {response.status_code}")
            if response.status_code == 200:
                    print(f"[SUCCESS] Successfully logo rendered")
                    self.success=True
                    return self.success
            if response.status_code == 404:
                    print(f"[FAILED] Channel UID not found")
                    self.logger.warning("Channel UID not found")
                    return self.success
            else:
                    print(f"[ERROR] Failed to render logo. Status Code: {response.status_code}")
                    self.logger.warning("Failed to render logo. Status Code: {response.status_code}")
                    return self.success
                    
        except requests.exceptions.RequestException as e:
            print(f"[EXCEPTION] A RequestException occurred: {e}")
            # self.logger.error(f"A RequestException occurred: {e}")
            return self.success
        except Exception as ex:
            print(f"[EXCEPTION] An unexpected error occurred: {ex}")
            self.logger.error("An unexpected error occurred: {ex}")
            return self.success