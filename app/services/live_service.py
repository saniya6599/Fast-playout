import json
import re
import requests
import socket
from CommonServices.Global_context import GlobalContext
from CommonServices.Logger import Logger
from CommonServices.DataFrameManager import DataFrameManager


# from Modules.melted.MeltedClient import MeltedClient


class live_service:

    def __init__(self):
        self.global_context = GlobalContext()
        self.logger=Logger()
        self.dataframe_manager = DataFrameManager()
        # self.client = MeltedClient()


    def process_live(self,df,pri_eventtime, scte_eventtime,scte_guid: dict):
        
        url = "http://172.15.2.56:8050/live_switch"
        
        # event_time = self.frames_to_time_format(self.parse_time_to_frames(pri_eventtime) + self.parse_time_to_frames(scte_eventtime))
        # scte_duration=self.frames_to_time_format(self.calculate_total_scte_frames(df,scte_guid))
        
        scte_data = {
            "channel_id":self.global_context.get_value('channel'),
            "Live_id":1
        }
    
        headers = {'Content-Type': 'application/json'}

        try:

            response = requests.post(url, data=json.dumps(scte_data), headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
                print(f"HTTP Error: {e.response.status_code}")
        except Exception as e:
            print(f"An error occurred: {str(e)}")
        print("Processing SCTE marker:", scte_data)
    
        return {"status": "success", "message": "SCTE marker alert sent!"}

    def process_live_tcp(self, df, pri_eventtime, scte_eventtime, scte_guid: dict):
        tcp_ip = self.global_context.get_value('tcp_ip')  
        tcp_port = self.global_context.get_value('tcp_port')  

        scte_data_str = f"MLT^_^LIVE^_^channel:{self.global_context.get_value('channel')}^_^Live_id:1"

        try:
           
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((tcp_ip, tcp_port))
                
                sock.sendall(scte_data_str.encode('utf-8'))
                
                response = sock.recv(1024).decode('utf-8')
                
                print(f"Response from server: {response}")
                
                return {"status": "success", "message": "Live switch command sent over TCP!"}

        except socket.error as e:
            raise Exception(f"Socket error: {str(e)}")
        except Exception as e:
            raise Exception(f"An error occurred: {str(e)}")


    def process_live_udp(self, Inventory):
        self.logger.info(f"Live ID detected, sending UDP Packet to : {self.global_context.get_value('cloudx_udp')}")
        
        udp_ip = self.global_context.get_value('cloudx_udp').split(':')[0]  
        udp_port = int(self.global_context.get_value('cloudx_udp').split(':')[1])
        
        live_id = re.findall(r"LIVE\s*(\d+)", Inventory)[0]
        
        scte_data_str = f"{self.global_context.get_value('channel_name')}^_^SWITCH-LIVE^_^{live_id}^_^<EOF-WIN>"

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                
                self.logger.info(f"Sending Live switch packet : {scte_data_str}")
                print(f"Sending Live switch packet : {scte_data_str}")
                response= sock.sendto(scte_data_str.encode('utf-8'), (udp_ip, udp_port))
                print(response)
                
                print("Live switch command sent over UDP")
                self.logger.info("Live switch command sent over UDP")
                
                
                #set  counter to check if live is on or not
                self.global_context.set_value('IsLiveRunning',True)

                # #reset melted after it has been switched to live
                # self.client.reset_melted()
                
                return {"status": "success", "message": "Live switch command sent over UDP!"}

        except socket.error as e:
            self.logger.info(f"Socket error: {str(e)}")
        except Exception as e:
            self.logger.info(f"An error occurred while sending live packet : {str(e)}")


    def parse_time_to_frames(time_str):
        try:
            # Assuming time format is HH:mm:ss:ff
            hours, minutes, seconds, frames = map(int, time_str.split(':'))
            total_frames = (hours * 3600 + minutes * 60 + seconds) * 25 + frames
            return total_frames
        except ValueError:
            return 0

    def frames_to_time_format(total_frames):
        hours = total_frames // (3600 * 25) % 24
        minutes = (total_frames % (3600 * 25)) // (60 * 25)
        seconds = (total_frames % (60 * 25)) // 25
        frames = total_frames % 25
        return f"{hours:02}:{minutes:02}:{seconds:02}:{frames:02}"
    
    def calculate_total_scte_frames(self,df, scte_guid: str):
    
        # Find the index of the row with the given SCTE GUID
        start_idx = df[df['guid'] == scte_guid].index[0]
        
        
        # Ensure that the SCTE GUID exists in the DataFrame
        if not start_idx:
            raise ValueError("SCTE GUID not found in the DataFrame.")
        
        # Initialize total frames
        total_frames = 0
        
        # Start the loop from the found index
        for i in range(start_idx, len(df)):
            row = df.iloc[i]
            
            # Break the loop if Type == "PRi" and Segment is not blank/null
            if row["Type"] == "PRI" and df.notna(row["Segment"]):
                break
            
            # Add the duration (in frames) if Type == "PRi" and Segment is blank/null
            if row["Type"] == "PRI" and df.isna(row["Segment"]):
                # Convert event time and duration to frames and add them
                duration_frames = self.parse_time_to_frames(row["duration"])
                total_frames += duration_frames
        
        return total_frames
    
    def get_live_sources(self):
        
        url = "http://172.15.2.56:8050/send_live_source"
        headers = {'Content-Type': 'application/json'}
        
        channel_id=self.global_context.get_value("channel_id")
        live_data={
            "channel_id": "admin3"
        }
        
        try:
            # response = requests.post(url,data=json.dumps(live_data), headers=headers,timeout=15)
            # data=response.json()
            # Live_sources= data.get("Live_sources")
            # channel_id_live=data.get("channel_id")
            # response.raise_for_status()
            
            #lives are ~ seperated : LIVE1|LiveType$url~LIVE2|LiveType$url
            #return f"MLT^_^GET-LIVE-PARAMETER^_^{Live_sources}"
            if(self.global_context.get_value('IsLive')):
                base_string = f"{self.global_context.get_value('channel_name')}^_^GET-LIVE-PARAMETER^_^"

                formatted_entries = []

                # Loop through each live entry and format it
                for i, entry in enumerate(self.global_context.get_value('live_config'), start=1):
                    label = entry['label'].strip()      
                    url= entry['url'].strip()
                    protocol= url.split("://")[0].upper()
                    formatted_entry = f"{label}|{protocol}${url}"
                    formatted_entries.append(formatted_entry)

                
                result_string = base_string + "~".join(formatted_entries)
                return result_string
            else:
                return ""
            #return response.json()
        except requests.HTTPError as e:
            print(f"HTTP Error: {e.response.status_code}")
        except Exception as e:
            print(f"An error occurred: {str(e)}")
        
        
    
        
        