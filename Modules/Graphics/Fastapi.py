import json
import re
import subprocess
import time
import requests
import os 
from CommonServices.Global_context import GlobalContext
from CommonServices.Logger import Logger

class   SPXGraphicsAPI:
    
    def __init__(self, base_url):
        self.base_url = base_url
        self.global_context= GlobalContext()
        self.logger=Logger()

    def get_projects(self):
        url = f"{self.base_url}/projects"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            print(f"HTTP Error: {e.response.status_code}")

    def get_rundowns(self, project):
        url = f"{self.base_url}/rundowns"
        params = {"project": project}
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            print(f"HTTP Error: {e.response.status_code}")

    def direct_playout(self,inventory,duration_Seconds):
        
        print(f"[INFO] Preparing to direct playout: {inventory}")
        self.logger.info(f"Preparing to direct playout: {inventory}")
        
        
        
        #json_file_path = os.path.join(os.path.dirname(__file__), f"{inventory}.json")
        #json_file_path = f'{self.global_context.get_value("melted_executable_path")}/{inventory}.json'
        
        url = f"{self.base_url}/api/v1/directplayout"
        headers = {'Content-Type': 'application/json'}
        

        try:    
            
            payload = self.generate_payload(inventory)
            
            print(f"[INFO] URL: {url}")
            print(f"[INFO] Payload: {payload}")
            self.logger.info(f"[INFO] URL: {url} with payload : {payload}")
            
            print(f"[INFO] Sending POST request to direct playout...")
            response = requests.post(url, json=payload, headers=headers)
            
            print(f"[INFO] Received response: {response.status_code}")
            self.logger.info(f"Received response: {response.status_code}")
            
            if response.status_code == 200:
               print(f"[INFO] Direct playout started for inventory '{inventory}'")
               self.logger.info(f"[INFO] Direct playout started for inventory '{inventory}'")
            else:
                print(f"[INFO] FAILED : Direct playout for inventory '{inventory}'")
                self.logger.info(f"[INFO] FAILED: Direct playout for inventory '{inventory}'")
                return
            
            
            # Wait for the duration before stopping all layers
            time.sleep(duration_Seconds)
            
            self.direct_playout_out(inventory)
            print(f"[INFO] Direct playout stopped for inventory '{inventory}'")
            self.logger.info(f"Direct playout stopped for inventory '{inventory}'")
        
            
        except requests.exceptions.RequestException as e:
            print(f"[EXCEPTION] A RequestException occurred: {e}")
            self.logger.error(f"A RequestException occurred: {e}")
        except Exception as ex:
            print(f"[EXCEPTION] An unexpected error occurred: {ex}")
            self.logger.error(f"An unexpected error occurred: {ex}")
       
    def direct_playout_out(self,inventory):
        
        url = f"{self.base_url}/api/v1/directplayout"
        #json_file_path = os.path.join(os.path.dirname(__file__), f"{inventory}.json")
        json_file_path = f'{self.global_context.get_value("melted_executable_path")}/{inventory}.json'
        headers = {'Content-Type': 'application/json'}

        try:
            # Generate the payload dynamically
            payload = self.generate_payload(inventory)
            payload['command']="stop"
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            return response.json()
        except FileNotFoundError:
            print(f"File not found: {json_file_path}")
           
            
        except requests.HTTPError as e:
                print(f"HTTP Error in direct playout out : {e.response.status_code}")
                #raise Exception(f"HTTP Error: {e.response.status_code} {e.response.text}")
        except Exception as e:
            print(f"An error occurred in direct playout out: {str(e)}")
            #raise Exception(f"An error occurred: {str(e)}")
       
    def stop_all_layers(self):
        url = f"{self.base_url}/api/v1/rundown/stopAllLayers"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            print(f"HTTP Error: {e.response.status_code}")
        except Exception as e:
            print(f"An error occurred: {str(e)}")
         
    def Panic(self):
        url = f"{self.base_url}/api/v1/panic"
        try:
            response = requests.get(url)

            if response.status_code == 200:
                self.logger.info(f"[INFO] Panic mode activated successfully for all graphics.")
                print(f"[INFO] Panic mode activated successfully for all graphics.")
            else:
                print(f"[INFO] FAILED: Panic mode operation")
                self.logger.info(f"FAILED: Panic mode operation")
                self.logger.error("FAILED: Panic mode operation")
                
        except requests.exceptions.RequestException as e:
            print(f"[EXCEPTION] A RequestException occurred: {e}")
            self.logger.error(f"A RequestException occurred: {e}")
        except Exception as ex:
            print(f"[EXCEPTION] An unexpected error occurred: {ex}")
            self.logger.error(f"An unexpected error occurred: {ex}")
        
    def Firelogo(self):
        
        
        print(f"[INFO] Preparing to fire logo")
        self.logger.info(f"Preparing to fire logo")
        
        url = f"{self.base_url}/api/v1/directplayout"
        
        #json_file_path = os.path.join(os.path.dirname(__file__), f"{inventory}.json")
        #json_file_path = f'{self.global_context.get_value("melted_executable_path")}/LOGO.json'
        
        headers = {'Content-Type': 'application/json'}

        try:
            
            payload = self.generate_payload("LOGO")
            
            print(f"[INFO] URL: {url}")
            print(f"[INFO] Payload: {payload}")
            
            self.logger.info(f"[INFO] URL: {url} with payload : {payload}")
            response = requests.post(url, json=payload, headers=headers)

            print(f"[INFO] Received response: {response.status_code}")
            self.logger.info(f"Received response: {response.status_code}")
            
            if response.status_code == 200:
               print("[INFO] Logo rendered successfully")
               self.logger.info("[INFO] Logo rendered successfully")
            else:
                print("[INFO] FAILED : Logo rendering operation'")
                self.logger.info("[INFO] FAILED : Logo rendering operation")
                self.logger.error("[INFO] FAILED : Logo rendering operation")
            
            
        except requests.exceptions.RequestException as e:
            print(f"[EXCEPTION] A RequestException occurred: {e}")
            self.logger.error(f"A RequestException occurred: {e}")
        except Exception as ex:
            print(f"[EXCEPTION] An unexpected error occurred: {ex}")
            self.logger.error(f"An unexpected error occurred: {ex}")
      
    def Stop_logo(self):
        
        print(f"[INFO] Preparing to stop logo")
        self.logger.info(f"Preparing to stop logo")
        
        url = f"{self.base_url}/api/v1/directplayout"
        #json_file_path = os.path.join(os.path.dirname(__file__), f"{inventory}.json")
        #json_file_path = f'{self.global_context.get_value("melted_executable_path")}/LOGO.json'
        
        headers = {'Content-Type': 'application/json'}
        
        try:
           
            payload = self.generate_payload("LOGO")
            payload['relativeTemplatePath']="/softpix/Template_Pack_1.1/blank.html"
            
            print(f"[INFO] URL: {url}")
            print(f"[INFO] Payload: {payload}")
            
            self.logger.info(f"[INFO] URL: {url} with payload : {payload}")
            
            response = requests.post(url, json=payload, headers=headers)
            
            if response.status_code == 200:
               print("[INFO] Logo stopped successfully")
               self.logger.info("[INFO] Logo stopped successfully")
            else:
                print("[INFO] FAILED : Logo stop operation'")
                self.logger.info("[INFO] FAILED : Logo stop operation")
                self.logger.error("[INFO] FAILED : Logo stop operation")
            
            
        except requests.exceptions.RequestException as e:
            print(f"[EXCEPTION] A RequestException occurred: {e}")
            self.logger.error(f"A RequestException occurred: {e}")
        except Exception as ex:
            print(f"[EXCEPTION] An unexpected error occurred: {ex}")
            self.logger.error(f"An unexpected error occurred: {ex}")
   
    def generate_payload(self,Inventory):
      try:

        # Adjust based on inventory
        if Inventory == "LOGO":
            caspar_channel = "1"
            caspar_layer = "1"
            webplayout_layer = "1"
            value=""
            template=self.global_context.get_value("logo")
        elif Inventory == "ASTON":
            caspar_channel = "2"
            caspar_layer = "2"
            webplayout_layer = "2"
            value = "PLANETCAST DEMO"
            template=self.global_context.get_value("aston")
        elif Inventory == "TICKER":
            caspar_channel = "3"
            caspar_layer = "3"
            webplayout_layer = "3"
            value="/excel/ticker/ticker_facts.xlsx"
            template="TICKER_EXCEL.html"


        payload = {
            "casparServer": "OVERLAY",  
            "casparChannel": caspar_channel,
            "casparLayer": caspar_layer,
            "webplayoutLayer": webplayout_layer,    
            "prepopulated": "false",
            "steps": "2",
            "out": "5000",
            "uicolor": "7",
            "relativeTemplatePath": f"/softpix/Template_Pack_1.1/{template}",
            "DataFields": [
                {"field": "f0", "value": value},
                {"field": "f1", "value": "Melted"},
                {
                    "field": "f99",
                    "ftype": "filelist",
                    "title": "Visual theme",
                    "extension": "css",
                    "value": "./themes/default.css"
                }
            ],
            "command": "play"
        }
        return payload
    
      except Exception as e:
          print(f"[ERROR] Error generating payload for inventory '{Inventory}': {e}")
          self.logger.error(f"[ERROR] Error generating payload for inventory '{Inventory}': {e}")
          return None
        
    # def create_dynamic_json(inventory):
    # # Define the base JSON structureusage
    
    #  json_data = {
    #     "playserver": "OVERLAY",
    #     "playchannel": "2",
    #     "playlayer": "16",
    #     "webplayout": "15",
    #     "steps": "2",
    #     "out": "5000",
    #     "uicolor": "7",
    #     "relativeTemplatePath": f"/softpix/Template_Pack_1.1/{inventory}.html",
    #     "DataFields": [
    #         {"field": "f0", "value": "LATER"},
    #         {"field": "f1", "value": "Commercial 3"},
    #         {    #             "field": "f99",
    #             "ftype": "filelist",
    #             "title": "Visual theme",
    #             "extension": "css",
    #             "value": "./themes/default.css"
    #         }
    #     ],
    #     "command": "play"
    #  }

    #  return json_data


    # def Fire_logo_ffmpeg(self):
        
    #     print("Firing logo through FFmpeg")
        
    #     position="top-right"
    #     input_video=self.global_context.get_value("melted_target")
    #     # output_video = re.sub(r":(\d+)$", lambda m: f":{int(m.group(1)) + 1}", self.global_context.get_value("melted_target"))
    #     output_video = re.sub(r"(udp://[0-9.]+:)(\d+)", lambda m: f"{m.group(1)}{int(m.group(2))+1}", self.global_context.get_value("melted_target"))

    #     logo_file=self.global_context.get_value("logo_file")
        
    #     # Default overlay position coordinates
    #     overlay_positions = {
    #         "top-right": "main_w-overlay_w-10:10",
    #         "top-left": "10:10",
    #         "bottom-right": "main_w-overlay_w-10:main_h-overlay_h-10",
    #         "bottom-left": "10:main_h-overlay_h-10",
    #     }

    #     # Validate position
    #     if position not in overlay_positions:
    #         raise ValueError(f"Invalid position: {position}. Choose from {list(overlay_positions.keys())}.")

    #     # Construct the FFmpeg command
    #     overlay_position = overlay_positions[position]
    #     # command = (
    #     #     "ffmpeg -i {input_video} -i {logo_file} -filter_complex overlay={overlay_position} -c:vlibx264 -preset fast -crf 23 -f mpegts output_video          # Output video file
    #     # )

    #     # Execute the FFmpeg command
    #     try:
    #         subprocess.run(command, check=True)
    #         print(f"Logo successfully overlayed. Output saved to {output_video}")
    #     except subprocess.CalledProcessError as e:
    #         print(f"Error while overlaying logo: {e}")

        
# # Example usage
# inventory = "NOW"
# resulting_json = create_dynamic_json(inventory)

# # Convert the result to a JSON string if needed
# json_string = json.dumps(resulting_json, indent=2)
# print(json_string)
