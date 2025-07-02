import json
import socket
import time
from flask import jsonify
import pandas as pd
import requests
from CommonServices.Global_context import GlobalContext
from CommonServices.DataFrameManager import DataFrameManager
from CommonServices.Time_management import timemgt
from CommonServices.Logger import Logger

class ScteService:
    def __init__(self):
        #logger=Logger()
        self.global_context=GlobalContext()
        self.df_manager = DataFrameManager()


    logger=Logger()

    def process_scte_marker(self,df,pri_eventtime, scte_eventtime,scte_guid: dict,inventory,primary_duration,scte_duration):
        
        
        print(f"[INFO] Preparing to trigger scte service")
        self.logger.info(f"Preparing to trigger scte service")
        # Logger.info(f'{inventory} event triggered for id 12345: SPLICE IMMEDIATE')
        #url = "http://172.15.2.56:8050/scte_hit"   #device manager
        # url = f"http://{self.global_context.get_value('scte_address')}/scte_trigger_api"   #packager from config.json 
        url = f"{self.global_context.get_value('gfx_server')}/scte_trigger_api"
        
        # event_time = self.frames_to_time_format(self.parse_time_to_frames(pri_eventtime) + self.parse_time_to_frames(scte_eventtime))
        event_time = self.frames_to_time_format(self.parse_time_to_frames(pri_eventtime) + self.parse_time_to_frames(primary_duration))
        Commercial_duration=  (self.calculate_total_scte_frames(scte_guid))/25 if scte_duration=="00:00:00:00" else self.parse_time_to_frames(scte_duration)/25
        print(f"Total Commercial duration: {Commercial_duration}")
        self.logger.info(f"Total Commercial duration: {Commercial_duration}")
        # scte_duration=self.frames_to_time_format(self.calculate_total_scte_frames(df,scte_guid))
        splice_immediate =  True if "ON" in inventory else False
        
        scte_data={
        "channel_uid": f'{self.global_context.get_value("channel_id")}',
        "duration": Commercial_duration,
        "pts": ""
        }
        
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Basic Zm9yYmVzOmZvcmJlc0BhcGk='
            }

        while True:
           
            #event_time_frames = self.parse_time_to_frames(event_time)
            #event_time_frames = self.parse_time_to_frames(event_time) - self.global_context.get_value("scte35_offset") if self.global_context.get_value("scte35_offset").startswith('-') else self.parse_time_to_frames(event_time) + int(self.global_context.get_value("scte35_offset"))
            event_time_frames = self.parse_time_to_frames(event_time) + int(self.global_context.get_value("scte35_offset"))
            system_time = timemgt.get_system_time_ltc() 
            system_time_frames = self.parse_time_to_frames(system_time)
            

            if system_time_frames >= event_time_frames:
    
                try:
                    print(f"Scte payload sent : {scte_data} at {system_time}")
                    response = requests.post(url, data=json.dumps(scte_data), headers=headers)
                    response.raise_for_status()
                    result= response.json()
                    
                    if response.status_code == 200:
                        print("Scte trigger : success")
                        print(f"Scte payload sent : {scte_data} at {system_time}")
                        self.logger.info(f"SUCCESS | Scte payload sent : {scte_data} at {system_time}")
                        self.global_context.set_value("IsScteOn", True)
                        # self.logger.info("ISSCTEON set to true")
                        
                    if response.status_code == 404:
                        print(f"Event not mapped at packager with channael ID : {self.global_context.get_value('channel_id')}")
                        self.logger.error(f"Event not mapped at packager with channael ID : {self.global_context.get_value('channel_id')}")
                        
                    if response.status_code == 400:
                        self.logger.warning("SCTE Service is off, Try enabling it!")

                    return response.json()
                except requests.HTTPError as e:
                        print(f"HTTP Error: {e.response.status_code}")
                        self.logger.error(f"HTTP Error at Scte insertion: {e.response.status_code}")
                        break
                
                except Exception as e:
                    print(f"An error occurred: {str(e)}")
                    self.logger.error(f"Error at Scte insertion: {e.response.status_code}")
                    break


    def restart_service(self):  #wrt packager 
        try:
           
            print(f"[INFO] Preparing to restart service")
            url = f"http://{self.global_context.get_value('scte_address')}/restartEvent"
        
            payload={
            'channel_uid': self.global_context.get_value('channel_id')
            }

            # Log the request details
            print(f'[INFO] URL: {url}')
            print(f'[INFO] Payload: {payload}')

            # Send the POST request
            print(f"[INFO] Sending POST request to restart service...")
            response = requests.request("POST", url,data=payload)
              # Log the response details
            print(f"[INFO] Received response: {response.status_code}")
            if response.status_code == 200:
                print(f"[SUCCESS] Successfully service restarted")
            else:
                print(f"[ERROR] Failed to restart service. Status Code: {response.status_code}")
            
        except requests.exceptions.RequestException as e:
            print(f"[EXCEPTION] A RequestException occurred: {e}")
        except Exception as ex:
            print(f"[EXCEPTION] An unexpected error occurred: {ex}")
        # finally:
        #     print(f"[INFO] Restart Service operation completed.")

    def process_scte_marker_tcp(self, df, pri_eventtime, scte_eventtime, scte_guid: dict, inventory):
        print("processing scte marker!")
        
        # TCP connection details
        tcp_ip = self.global_context.get_value('tcp_ip')  # IP address of the server
        tcp_port = self.global_context.get_value('tcp_port')  # Port number of the server

        event_time = self.frames_to_time_format(
            self.parse_time_to_frames(pri_eventtime) + self.parse_time_to_frames(scte_eventtime)
        )
        splice_immediate = True if "ON" in inventory else False
        
        scte_data = {
            "channel_name": "admin3",
            "splice_event_id": 12345,
            "splice_immediate": splice_immediate
        }
        
        while True:
            system_time = timemgt.get_system_time_ltc() 
            system_time_frames = self.parse_time_to_frames(system_time)
            event_time_frames = self.parse_time_to_frames(event_time)
            
            if system_time_frames >= event_time_frames:
                try:
                    # Create a TCP socket
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                        sock.connect((tcp_ip, tcp_port))
                        
                        json_data = json.dumps(scte_data)
                        
                        sock.sendall(json_data.encode('utf-8'))
                        
                        response = sock.recv(1024).decode('utf-8')
                        
                        result = json.loads(response)
                        
                        channel = result.get("channel_name")
                        message = result.get("message")
                        splice_event_id = result.get("splice_event_id")
                        splice_immediate_result = result.get("splice_immediate")
                        
                        if "successfully" in message:
                            print(f"SCTE triggered with event ID : {splice_event_id} at {system_time}")
                        return result

                except socket.error as e:
                    raise Exception(f"Socket error: {str(e)}")
                except Exception as e:
                    raise Exception(f"An error occurred: {str(e)}")

            time.sleep(0.01)  # Small sleep to avoid overloading the CPU

    def parse_time_to_frames(self,time_str):
            try:
                # Assuming time format is HH:mm:ss:ff
                hours, minutes, seconds, frames = map(int, time_str.split(':'))
                total_frames = (hours * 3600 + minutes * 60 + seconds) * 25 + frames
                return total_frames
            except ValueError:
                return 0

    def process_scte_udp_On(self, df, pri_eventtime, scte_eventtime, scte_guid: dict, inventory):
        
        event_time = self.frames_to_time_format(self.parse_time_to_frames(pri_eventtime) + self.parse_time_to_frames(scte_eventtime))
 
        udp_ip = self.global_context.get_value('cloudx_udp').split(':')[0]
        udp_port = int(self.global_context.get_value('cloudx_udp').split(':')[1])
        
        print(f"SCTE TYPE : {self.global_context.get_value('scte_type')} at {udp_ip}:{udp_port}")

        scte_nature= "ON"
        Commercial_duration=self.frames_to_time_format(self.calculate_total_scte_frames(df,scte_guid))
        duration = "00:00:00:00" if self.global_context.get_value('scte_type') == "IMMEDIATE" else Commercial_duration
        scte_data_str = f"{self.global_context.get_value('channel_name')}^_^INSERTSCTE-35^_^{self.global_context.get_value('scte_type')}~{scte_nature}~{duration}^_^EOF-WIN"
        
        while True:
            system_time = timemgt.get_system_time_ltc() 
            system_time_frames = self.parse_time_to_frames(system_time)
            event_time_frames = self.parse_time_to_frames(event_time)
            

            if system_time_frames >= event_time_frames:
    
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

                        response= sock.sendto(scte_data_str.encode('utf-8'), (udp_ip, udp_port))
                        from Modules.melted.MeltedClient import MeltedClient
                        self.logger.info(response)
                        self.logger.info(f"SCTE ON Trigger command sent at {(timemgt.get_system_time_ltc())}")
                        self.logger.info(f"Scte packet detail sent : {scte_data_str}")
                        
                        self.global_context.set_value("IsScteOn", True)
                        # self.logger.info("ISSCTEON set to true")
                        
                        return {"statufrom Modules.melted.MeltedClient import MeltedClients": "success", "message": "SCTE ON Trigger command sent over UDP!"}

                except socket.error as e:
                    raise Exception(f"Socket error: {str(e)}")
                except Exception as e:
                    raise Exception(f"An error occurred: {str(e)}")

    def process_scte_udp_off(self):
        
        #scte_type= "IMMEDIATE" if ("ON" or "OFF") in inventory else "SCHEDULED"
        inventory="SCTEOFF"
        udp_ip = self.global_context.get_value('cloudx_udp').split(':')[0]
        udp_port = int(self.global_context.get_value('cloudx_udp').split(':')[1])
        
        print(f"SCTE TYPE : {self.global_context.get_value('scte_type')} at {udp_ip}:{udp_port}")

        scte_nature= "OFF"
        scte_data_str = f"{self.global_context.get_value('channel_name')}^_^INSERTSCTE-35^_^{self.global_context.get_value('scte_type')}~{scte_nature}~00:00:00:00^_^EOF-WIN"
        

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

                response= sock.sendto(scte_data_str.encode('utf-8'), (udp_ip, udp_port))
                
                self.logger.info(response)
                self.logger.info("status : success , message: SCTE OFF Trigger command sent over UDP!")
                self.logger.info(f"Scte packet detail sent : {scte_data_str}")
                
                self.global_context.set_value("IsScteOn", False)
                self.logger.info("ISSCTEON set to false")
                
                return {"status": "success", "message": "SCTE OFF Trigger command sent over UDP!"}

        except socket.error as e:
            raise Exception(f"Socket error: {str(e)}")
        except Exception as e:
            raise Exception(f"An error occurred: {str(e)}")



    def frames_to_time_format(self,total_frames):
        hours = total_frames // (3600 * 25) % 24
        minutes = (total_frames % (3600 * 25)) // (60 * 25)
        seconds = (total_frames % (60 * 25)) // 25
        frames = total_frames % 25
        return f"{hours:02}:{minutes:02}:{seconds:02}:{frames:02}"
    
    def calculate_total_scte_frames(self, scte_guid: str):
    
    
        df = self.df_manager.get_dataframe()
        # Find the index of the row with the given SCTE GUID
        start_idx = df[df['GUID'] == scte_guid].index[0]
        
        # Ensure that the SCTE GUID exists in the DataFrame
        if not start_idx:
            raise ValueError("SCTE GUID not found in the DataFrame.")
        
        # Initialize total frames
        total_frames = 0
        
        # Start the loop from the found index
        for i in range(start_idx, len(df)):
            row = df.iloc[i]
            
            # Break the loop if Type == "PRi" and Segment is not blank/null
            if row["Type"] == "PRI" and row["Segment"] != '':
                break
            
            # Add the duration (in frames) if Type == "PRi" and Segment is blank/null
            if row["Type"] == "PRI" and pd.notna(row["Segment"]):
                # Convert event time and duration to frames and add them
                duration_frames = self.parse_time_to_frames(row["Duration"])
                total_frames += duration_frames
        
        return total_frames
    
    
    # def trigger_api_at_eventtime(timemgt, eventtime_str, api_callback):
    #     # Convert eventtime to frames
    #     event_time_frames = parse_time_to_frames(eventtime_str)
        
    #     while True:
    #         # Get system LTC time and convert it to frames
    #         systemtime = timemgt.get_system_time_ltc()  # Assuming this returns a string in HH:mm:ss:ff
    #         system_time_frames = parse_time_to_frames(systemtime)

    #         # Check if system time matches or exceeds the event time
    #         if system_time_frames >= event_time_frames:
    #             # Trigger the API call
    #             api_callback()
    #             break

    #         # Small sleep to avoid overloading CPU in the loop
    #         time.sleep(0.01)  # Sleep for 10 milliseconds

   