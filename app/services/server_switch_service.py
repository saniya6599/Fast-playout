import json
import re
import requests
import socket
from CommonServices.Global_context import GlobalContext
from CommonServices.Logger import Logger
from CommonServices.DataFrameManager import DataFrameManager


# from Modules.melted.MeltedClient import MeltedClient


class server_switch_service():
    
    def __init__(self):
        self.global_context = GlobalContext()
        self.logger=Logger()
        self.dataframe_manager = DataFrameManager()
   
    def execute(self):
        self.logger.info(f"SERVER-SWITCH TRIGGER, sending UDP Packet to : {self.global_context.get_value('cloudx_udp')}")
        
        udp_ip = self.global_context.get_value('cloudx_udp').split(':')[0]  
        udp_port = int(self.global_context.get_value('cloudx_udp').split(':')[1])
        
        
        command = f"{self.global_context.get_value('channel_name')}^_^SWITCH-SERVER^_^<EOF-WIN>"

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                
                self.logger.info(f"Sending server switch packet : {command}")
                print(f"Sending server switch packet : {command}")
                response= sock.sendto(command.encode('utf-8'), (udp_ip, udp_port))
                print(response)
                
                print("Server switch command sent over UDP")
                self.logger.info("Server switch command sent over UDP")
                

                # #reset melted after it has been switched to live
                # self.client.reset_melted()
                
                return {"status": "success", "message": "Live switch command sent over UDP!"}

        except socket.error as e:
            self.logger.info(f"Socket error: {str(e)}")
        except Exception as e:
            self.logger.info(f"An error occurred while sending live packet : {str(e)}")

