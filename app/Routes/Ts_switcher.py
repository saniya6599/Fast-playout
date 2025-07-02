import os
import re
import time
import pandas as pd
import requests
from CommonServices.Global_context import GlobalContext
from CommonServices.DataFrameManager import DataFrameManager
from CommonServices.Logger import Logger


class switcher:
    
    def __init__(self,base_url):
        self.global_context = GlobalContext()
        self.df_manager = DataFrameManager()
        self.base_url = base_url
        self.logger=Logger()    

    def add_service(self):
        url = f"{self.base_url}/addservice"
        payload = self.get_payload("add_service","")
        files=[]
        headers = {}
        print(f"[INFO] Adding service with payload : {payload}")
        self.logger.info(f"Adding service with payload : {payload}")
        response = requests.request("POST", url, headers=headers, data=payload, files=files)
        print(f"[INFO] Response : {response}")

    def update_service(self, updated_live):
        url = f"{self.base_url}/addservice"
        payload = self.get_payload("update_service",updated_live)
        files=[]
        headers = {}

        response = requests.request("POST", url, headers=headers, data=payload, files=files)
        print(response)

    def get_service_id(self, payload):
        url = f"{self.base_url}/get_channel_id"
        payload = self._manipulate_payload(payload, "get_service_id")
        return self._post_request(url, payload)

    def delete_service(self, payload):
        url = f"{self.base_url}/deleteScan"
        payload = self._manipulate_payload(payload, "delete_service")
        return self._post_request(url, payload)

    def add_user(self, payload):
        url = f"{self.base_url}/allusers"
        payload = self._manipulate_payload(payload, "add_user")
        return self._post_request(url, payload)

    def update_user(self, payload):
        url = f"{self.base_url}/allusers"   
        return self._post_request(url, payload)

    def start_service(self):
        try:
           
            print(f"[INFO] Preparing to start service")
            self.logger.info(f"Preparing to start service")
            url = f"{self.base_url}/startStop"
        
            payload={
            'name': self.global_context.get_value('channel_name'),
            'status': '0',
            'username': 'hub'
            }
            files=[]
            headers = {}

            # Log the request details
            print(f"[INFO] URL: {url}")
            print(f"[INFO] Payload: {payload}")
            self.logger.info(f"[INFO] URL: {url} with payload : {payload}")

            # Send the POST request
            print(f"[INFO] Sending POST request to start service...")
            response = requests.request("POST", url, headers=headers, data=payload, files=files)
              # Log the response details
            print(f"[INFO] Received response: {response.status_code}")
            self.logger.info(f"Received response: {response.status_code}")
            if response.status_code == 200:
                print(f"[SUCCESS] Successfully started service")
                self.logger.info(f"Successfully started service")

            else:
                print(f"[ERROR] Failed to start service. Status Code: {response.status_code}")
                self.logger.error(f"Failed to start service. Status Code: {response.status_code}.")
            
        except requests.exceptions.RequestException as e:
            print(f"[EXCEPTION] A RequestException occurred: {e}")
            self.logger.error(f"A RequestException occurred: {e}")
        except Exception as ex:
            print(f"[EXCEPTION] An unexpected error occurred: {ex}")
            self.logger.error(f"An unexpected error occurred: {ex}")
        # finally:
        #     print(f"[INFO] Start Service operation completed.")



    def stop_service(self):
        try:
           
            print(f"[INFO] Preparing to stop service")
            self.logger.info(f"Preparing to stop service")
            url = f"{self.base_url}/startStop"
        
            payload={
            'name': self.global_context.get_value('channel_name'),
            'status': '1',
            'username': 'hub'
            }
            files=[]
            headers = {}

            # Log the request details
            print(f"[INFO] URL: {url}")
            print(f"[INFO] Payload: {payload}")
            self.logger.info(f"[INFO] URL: {url} with payload : {payload}")

            # Send the POST request
            print(f"[INFO] Sending POST request to stop service...")
            response = requests.request("POST", url, headers=headers, data=payload, files=files)
              # Log the response details
            print(f"[INFO] Received response: {response.status_code}")
            self.logger.info(f"Received response: {response.status_code}")
            if response.status_code == 200:
                print(f"[SUCCESS] Successfully stopped service")
                self.logger.info(f"Successfully stopped service")
            else:
                print(f"[ERROR] Failed to stop service. Status Code: {response.status_code}")
                self.logger.error(f"Failed to stop service. Status Code: {response.status_code}")
            
        except requests.exceptions.RequestException as e:
            print(f"[EXCEPTION] A RequestException occurred: {e}")
            self.logger.error(f"A RequestException occurred: {e}")
        except Exception as ex:
            print(f"[EXCEPTION] An unexpected error occurred: {ex}")
            self.logger.error(f"An unexpected error occurred: {ex}")
        # finally:
        #     print(f"[INFO] Stop Service operation completed.")
       
    def switch_service(self, switchto):
        try:
            service_type = "LIVE" if switchto == 1 else "SERVER"
            print(f"[INFO] Preparing to switch service to: {service_type}")
            self.logger.info(f"[Preparing to switch service to: {service_type}")

            url = f"{self.base_url}/chstreamtype"
            payload = {
                'chname':  self.global_context.get_value('channel_name'),
                'stream_status': str(switchto)
            }
            files = []
            headers = {}

            print(f"[INFO] URL: {url}")
            print(f"[INFO] Payload: {payload}")
            self.logger.info(f"[INFO] URL: {url} with payload : {payload}")


            print(f"[INFO] Sending POST request to switch service...")
            response = requests.request("POST", url, headers=headers, data=payload, files=files)

            print(f"[INFO] Received response: {response.status_code}")
            self.logger.info(f"Received response: {response.status_code}")
            if response.status_code == 200:
                print(f"[SUCCESS] Successfully switched to: {service_type}")
                self.logger.info(f"Successfully Switched to: {service_type}")
            else:
                print(f"[ERROR] Failed to switch to {service_type}. Status Code: {response.status_code}")
                self.logger.error(f"Failed to switch to {service_type}. Status Code: {response.status_code}.")

        except requests.exceptions.RequestException as e:
            print(f"[EXCEPTION] A RequestException occurred: {e}")
            self.logger.error(f"A RequestException occurred: {e}")
        except Exception as ex:
            print(f"[EXCEPTION] An unexpected error occurred: {ex}")
            self.logger.error(f"An unexpected error occurred: {ex}")
        finally:
            print(f"[INFO] Switch Service operation completed.")

    # def _post_request(self, url, payload):
    #     try:
    #         response = requests.post(url, data=payload)
    #         response.raise_for_status()
    #         return response.json()
    #     except requests.RequestException as e:
    #         print(f"Error while making POST request to {url}: {e}")
    #         return None



# # Example usage
# if __name__ == "__main__":
#     base_url = "http://192.168.15.253:8090"
#     api = MeltedManagerAPI(base_url)


    def get_payload(self,service,data):

        melted_target = self.global_context.get_value("melted_target")
        melted_ip, melted_port = (re.search(r'udp://([\d.]+):(\d+)', self.global_context.get_value('melted_target')).groups() if re.search(r'udp://([\d.]+):(\d+)', self.global_context.get_value('melted_target')) else (None, None))
        live=self.global_context.get_value("live_config")[0]["url"]
      

        if (service.startswith("add")):
            tag = "add"
            live=self.global_context.get_value("live_config")[0]["url"]
            print(f"adding service in packager with main server  : {self.global_context.get_value('melted_target')} and backup with 1st live : {live}" )
            live_ip,live_port = (re.search(r'udp://([\d.]+):(\d+)',live).groups() if re.search(r'udp://([\d.]+):(\d+)', live) else (None, None))
            
        elif (service.startswith("update")):
            tag="update"
            live_ip,live_port = (re.search(r'udp://([\d.]+):(\d+)',data).groups() if re.search(r'udp://([\d.]+):(\d+)', data) else (None, None))
        
        
        return {
            'service_name': self.global_context.get_value('channel_name'),
            'service_source_ip': melted_ip,
            'service_source_port': melted_port,
            'service_bkp_source_ip': live_ip,
            'service_bkp_source_port': live_port,
            'service_destination_ip': self.global_context.get_value('switcher_dest').split(':')[0],
            'service_destination_port': self.global_context.get_value('switcher_dest').split(':')[1],
            'port_redundancy': 'true',
            'vpid_miss': 'false',
            'apid_miss': 'false',
            'cc_err': 'false',
            'bitrate_err': 'false',
            'video_black': 'false',
            'video_freeze': 'false',
            'audio_loss': 'false',
            'audio_loudness': 'false',
            'tag': tag,
            's_inf': f'ens5[{melted_ip}]',
            'd_inf': f'ens5[{melted_ip}]',
            'b_inf': f'ens5[{melted_ip}]',
            'username': 'test23'
        }

    # response = api.add_service(payload)
    # print(response)
