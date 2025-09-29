from datetime import date, datetime
import json
import os
import re
import sys
import threading
from flask import Flask, request, jsonify
import requests
from CommonServices.Global_context import GlobalContext
from Modules.melted.MeltedClient import MeltedClient
from CommonServices.Logger import Logger
from CommonServices.ConfigCreator import CustomDumper
from CommonServices.DataFrameManager import DataFrameManager
from CommonServices.auto_recovery import AutoRecovery
from Modules.Verify.Database_verification import Database_verification
from Modules.Verify.PlaylistVerifier import PlaylistVerifier
from CommonServices.AsrunGenerator import AsRunFormatter



app = Flask(__name__)

class APIListener:
    def __init__(self):
        self.out_mode = None
        self.status = None
        self.location = None
        self.settings = None
        self.start_server = None 
        self.start_melted=None
        self.channel=None
        self.global_context = GlobalContext()
        self.CustomDumper= CustomDumper()
        self.logger=Logger()
        self.recovery_data=None
        self.df=DataFrameManager()
        self.client=MeltedClient()
        self.recovery=AutoRecovery()
        self.db_verification = Database_verification()

    def start_api_server(self):
        
        @app.route('/api/isserverup', methods=['POST'])
        def IsServerUp():
            try:
                print("[INFO] Validating server existence")  
                return jsonify({"message": "Server is up"}), 200  
            except Exception as ex:
                print(f"[ERROR] Error while validating server: {ex}")  
                return jsonify({"error": "Internal Server Error"}), 500
            
        @app.route('/api/start', methods=['POST'])
        def start():
          try:
            print("[INFO] Received API request at /api/start")
            self.logger.info(" API request received at /api/start")
           
            data = request.get_json()
            
            if not data:
                error_msg = "No JSON data received in the request"
                print(f"[ERROR] {error_msg}")
                self.logger.error(error_msg)
                return jsonify({"message": error_msg, "status": "failure"}), 400

            channel_name = data.get("channel_name")
            channel_id = data.get("channel_id")
            host= data.get("machine_ip")
            port= data.get("machine_port")
            db_ip = data.get("db_ip")
            db_port = data.get("db_port")
            db_username = data.get("db_username")
            db_password = data.get("db_password")
            db_name = data.get("db_name")
            gfx = data.get("gfx")
            gfx_ip = data.get("gfx_ip") if gfx else None
            gfx_port = data.get("gfx_port") if gfx else None
            logo_hide_on = data.get("logo_settings")
            verification_mode = data.get("verification_mode")
            scte35 = data.get("scte35")
            scte35_type = data.get("scte35_type")
            scte35_offset = data.get("scte35_offset")
            scte_mode=data.get("scte_mode")
            
            auto_recovery=data.get("auto_recovery")
            
            playlist_loc = data.get("playlist_location")
            content_loc = data.get("content_location")
            asrun_loc = data.get("asrun_location")
            busfile_loc = data.get("busfile_location")
            
            log_retention_days = data.get("log_retention_days")

            verification_mode=data.get("verification_mode")
            
            auto_verification=data.get("auto_verification")
            
            asrun_filter=data.get("asrun_filter")

            external_config = data.get("external_config", [])
            output = data.get("output", {})
            IsLive = data.get("live")
            live_config = data.get("live_config", [])

            external_cofig_vars = [
                {"ip": config.get("ip"), "port": config.get("port"), "label": config.get("label")}
                for config in external_config
            ]

            out_mode = output.get("OutMode")
            out_status = output.get("status")
            out_location = output.get("location")
            start_server = output.get("StartServer")
            settings = output.get("settings", {})
            

            self.logger.info(f"Received settings for channel '{channel_name}'")
            self.logger.info(f"Received : {data}")
        


            # if not start_server : 
            #     client=MeltedClient() 
            #     client.kill_melted() 

            #self.check_and_set_permissions(self.location)

            if start_server:
                print(f"Outmode : {out_mode}")
                self.global_context.set_value("OutMode", out_mode)
                
                self.global_context.set_value("channel_name",channel_name)
                self.global_context.set_value("channel_id",channel_id)
                
                self.global_context.set_value("host",host)
                self.global_context.set_value("port",port)
                
                self.global_context.set_value("Database", f"{db_ip}:{db_port}:{db_name}")
                self.global_context.set_value("Database_cred", f"{db_username}:{db_password}")
                
                self.global_context.set_value("ContentLoc",content_loc)
                self.global_context.set_value("PlaylistLoc",playlist_loc)
                self.global_context.set_value("asrun_location",asrun_loc)
                self.global_context.set_value("busfile_location",busfile_loc)
                
                self.global_context.set_value("log_retention_days",log_retention_days)
                
                self.global_context.set_value("melted_target",settings["target"])
                
                self.global_context.set_value("IsGraphics",gfx)
                
                if(self.global_context.get_value("IsGraphics")):
                    self.global_context.set_value("gfx_server",f"http://{gfx_ip}:{gfx_port}")
                    print(f"[INFO] Graphics server set to: {self.global_context.get_value('gfx_server')}")
                    self.logger.info(f"Graphics server set to: {self.global_context.get_value('gfx_server')}")
                else:
                    print(f"[WARNINGdata] Graphics are disabled for channel: {channel_name}")
                    self.logger.warning(f"Graphics are disabled for channel: {channel_name}")
                    
                self.global_context.set_value("logo_hide_on",logo_hide_on)
                    
                self.global_context.set_value("IsScte",scte35)
                
                if(self.global_context.get_value("IsScte")):
                    self.global_context.set_value("scte_type",str(scte35_type).upper())
                    self.global_context.set_value("scte35_offset",scte35_offset)
                    self.global_context.set_value("scte_mode",scte_mode)
                    self.logger.info(f"SCTE enabled with type: {scte35_type}, mode: {scte_mode} & offset: {scte35_offset}")
                    print(f"[INFO] SCTE enabled with type: {scte35_type} & offset: {scte35_offset}")
                else:
                    print(f"[WARNING] SCTE insertions are disabled for channel: {channel_name}")
                    self.logger.warning(f"SCTE insertions are disabled for channel: {channel_name}")

                self.global_context.set_value("IsLive",IsLive)
                self.global_context.set_value("live_config",live_config) 
                self.global_context.set_value("verification_mode",verification_mode)
                self.global_context.set_value("auto_verification",auto_verification)
                
                self.global_context.set_value("Isauto_recovery",auto_recovery)
                self.global_context.set_value("asrun_filter",asrun_filter)
                
                #self.global_context.set_value("Channel_yml",self.location)    #removing it beacuse yml creates at melted_executable_path
                
                print(f"Creating YAML for channel '{channel_name}' with settings: {settings}")
                self.logger.info(f"Creating YAML for channel '{channel_name}' with settings: {settings}")
                try:
                        self.CustomDumper.Create(settings)
                        self.global_context.set_value("StartMelted", True)
                        print("[INFO] Playback server will be started.")
                        self.logger.info("Playback server will be started.")
                except Exception as e:
                        print(f"[EXCEPTION] Error creating YAML: {e}")
                        self.logger.error("Error creating YAML")
                        # return jsonify({"message": "Failed to create YAML", "status": "failure"}), 500


            return jsonify({"message": "Request processed successfully", "status": "success"}), 200
        
          except Exception as e:
                print(f"[ERROR] Unexpected error: {e}")
                self.logger.error("Unexpected error in APIListener start method")
                return jsonify({"message": "Internal server error", "status": "failure"}), 500
        
        
        @app.route('/api/pre_recovery', methods=['POST'])
        def start_with_recovery():
            try:

                    print("Recieved request for recovery")
                    data = request.get_json()

                    if not data:

                        return jsonify({"message": "No data provided", "status": "error"}), 400
                        print("message: No data provided,status: error")
                    
                    self.logger.info(f"Received pre-recovery data: {data}")
                    print(f"Received pre-recovery data: {data}")
                    

                    required_keys = ["status", "playlist", "previous_running_id", "current_reckonkey", "last_checked","elapsed_frames", "remaining_time"]
                    missing_keys = [key for key in required_keys if key not in data or data.get(key) in [None, ""]]

                    if missing_keys:
                        self.logger.info(f"Missing keys: {missing_keys}, trying to resolve from recovery file")
                        # Load recovery.json
                        try:
                            recovery_path=os.path.join(self.global_context.get_value('PlaylistLoc'),f'{self.global_context.get_value("channel_name")}_recovery.json')
                            with open(recovery_path, "r") as f:
                                recovery_entries = json.load(f)

                            # Ensure recovery_entries is a list
                            if not isinstance(recovery_entries, list):
                                recovery_entries = [recovery_entries]
 
                            matched_entry = next(
                                (entry for entry in recovery_entries if str(entry.get("last_checked")) == str(data.get("last_checked"))),
                                None
                            )

                            if matched_entry:
                                for key in missing_keys:
                                    if key in matched_entry:
                                        data[key] = matched_entry[key]

                                # Re-check if any keys still missing after patch
                                remaining_missing = [key for key in required_keys if key not in data]
                                if remaining_missing:
                                    return jsonify({"message": f"Still missing keys after recovery file patch: {remaining_missing}", "status": "error"}), 400
                            else:
                                return jsonify({"message": f"No matching recovery entry found for last_checked = {data.get('last_checked')}", "status": "error"}), 400

                        except Exception as e:
                            return jsonify({"message": f"Error reading recovery file: {e}", "status": "error"}), 500
                                        

                    self.recovery_data = data  
                    playlist = data.get("playlist")
                    previous_running_id = data.get("previous_running_id")
                    previous_running_index = data.get("previous_running_index")
                    current_reckonkey= data.get('current_reckonkey')
                    last_checked = data.get("last_checked")
                    recovery_som=data.get("elapsed_frames")
                    remaining_time=data.get("remaining_time")
                    
                    self.global_context.set_value("Playlist_loaded",playlist)
                    self.global_context.set_value("automsg","automation_stop")
                    self.global_context.set_value("recovery_som",recovery_som)

                    if_exists =  self.df.check_playlist(os.path.join(self.global_context.get_value("PlaylistLoc")), playlist)
                    # if_exists =  self.df.check_playlist(os.path.join(self.global_context.get_value("PlaylistLoc"),'Recovery'), playlist)
                    
                    if if_exists:
                        # Load the DataFrame from the specified file
                        print("Playlist exists!")
                        playlist_index = self.df.find_index_by_reckon_key(os.path.join(self.global_context.get_value("PlaylistLoc"), playlist), current_reckonkey)
                        self.df.load_dataframe(os.path.join(self.global_context.get_value("PlaylistLoc"), playlist),playlist_index)
                        # playlist_index = self.df.find_index_by_reckon_key(os.path.join(self.global_context.get_value("PlaylistLoc"),'Recovery', playlist), current_reckonkey)
                        # self.df.load_dataframe(os.path.join(self.global_context.get_value("PlaylistLoc"),'Recovery', playlist),playlist_index)
                        self.global_context.set_value('channel_props',playlist)
                        self.global_context.set_value('current_playlists', [playlist])
                        
                        #if database verification is on, verify before starting auto recovery
                        if not (self.global_context.get_value("verification_mode").startswith('Physical')):
                            self.logger.info("Database verification mode is on, Verifying before recovery playout!")
                            self.db_verification.verify()
                            self.global_context.set_value('sync',1)
                        else:
                            verifier = PlaylistVerifier()
                            verifier.full_playlist_verification()
                            self.global_context.set_value('sync',1)
                        
                       
                        df = self.df.get_dataframe()
                        startfrom=df.at[0,'GUID']
                        self.df.update_row(0, 'SOM', self.frames_to_time_format(int(recovery_som)))
                        self.df.update_row(0,'Duration', remaining_time)
                        
                        self.global_context.set_value("startfrom",startfrom)
                        self.global_context.set_value("sync",1)
                        print("Starting server with recovery data..")
                        self.logger.info("Starting server with recovery data..")
                        
                        if not self.client.start(startfrom) :
                            self.logger.error("Unable to start playback after recovery, try manually!")
                        else:
                            self.logger.info("Server started in recovery mode")
                            self.global_context.set_value("Is_server_in_recovery", True)
                            self.global_context.set_value("recovery_index",int(previous_running_index))
                            self.start_asrun_thread()

                    else:
                        print(f"Playlist in recovery data :{os.path.join(self.global_context.get_value('PlaylistLoc'), playlist)} doesn't exist!")
                        self.logger.error(f"Playlist in recovery data :{os.path.join(self.global_context.get_value('PlaylistLoc'), playlist)} doesn't exist!")
                        return jsonify({"message": f"Unable to recover server, playlist {os.path.join(self.global_context.get_value('PlaylistLoc'), playlist)}  doesn't exist!", "status": "Failed"}), 500

                    print("message: Pre-recovery data accepted and stored, status: success")
                    return jsonify({"message": "Pre-recovery data accepted and stored", "status": "success"}), 200
                     

            except Exception as e:
                    self.logger.error(f"Error processing pre-recovery data: {e}")
                    return jsonify({"message": "Failed to process pre-recovery data", "status": "error"}), 500
            
        #sending recovery on stop if set to true
        @app.route('/api/get_recovery_on_stop', methods=['POST'])
        def get_recovery_on_stop():
            try:
                
                # Flush remaining asrun entries
                self.flush_remaining_asrun_before_shutdown()
                self.recovery.write_recovery_playlist()

                # Fetch recovery data
                if(self.global_context.get_value('Isauto_recovery')):
                 recovery_data = self.settings_provider()
                 self.logger.info(f"sending recovery before terminating server : {recovery_data}")

                if not recovery_data:
                    self.logger.info("Auto recovery is disabled. Proceeding to shut down the server.")
                    print(" Auto recovery is disabled. Proceeding to shut down the server.")
                    return jsonify({"message": "No recovery data available"}), 400

                self.recovery.save_to_file(recovery_data)
                return json.dumps(recovery_data)
                
                if response.status_code == 200:
                    self.logger.info(f"Recovery data sent successfully: {response.json()}")
                    return jsonify({"message": "Recovery data sent successfully", "status": "success"}), 200
                else:
                    self.logger.error(f"Failed to send recovery data. Status Code: {response.status_code}, Response: {response.text}")
                    return jsonify({"message": "Failed to send recovery data", "status": "error"}), 500

            except Exception as e:
                self.logger.error(f"An error occurred while sending recovery data: {e}")
                return jsonify({"message": "Internal server error", "status": "error"}), 500
        
        
        # # fetch logs as per requirement
        @app.route('/api/fetch_logs', methods=['POST'])
        def fetch_logs():
            try:
               
                params = request.get_json()
                print(f"Recieved request for logs with params : {params}")
                logs = self.logger.get_logs(params)
                print(f"sending logs")

                if not logs:
                    self.logger.info("No logs generated for recieved criteria!")
                    print("No logs generated for recieved criteria!")
                    return jsonify({"message": "No Logs available"}), 400

                
                return json.dumps(logs)

            except Exception as e:
                self.logger.error(f"An error occurred while sending logs: {e}")
                return jsonify({"message": "Internal server error", "status": "error"}), 500
        
        @app.route('/api/get_current_playlist', methods=['POST'])
        def get_current_playlist():
            try:
                playlist = self.global_context.get_value('current_playlists')[-2:]

                self.logger.info(f"Fetched latest playlists loaded:{playlist}")
                return {"current_playlists": playlist}
            
            except Exception as e:
                self.logger.error("Error while fetching current_playlists")

            
        
        try:
            app.run(host='0.0.0.0', port=self.global_context.get_value('listener_port'))
        except Exception as e:
            print(f"[ERROR] Error starting API server: {e}")
            self.logger.error("Error starting API server")
            sys.exit(1)


    def check_and_set_permissions(path):
     
     path=f"{path.location[0]}.yml"
     if os.path.exists(path):
        print(f"Path exists: {path}")

        # Get the current permissions of the file/directory
        current_permissions = oct(os.stat(path).st_mode)[-3:]
        print(f"Current permissions: {current_permissions}")

        # Check if the permissions are 777
        if current_permissions != '777':
            print(f"Setting permissions to 777 for: {path}")
            os.chmod(path, 0o777)
        else:
            print("Permissions are already set to 777.")
     else:
        print(f"Path does not exist: {path}")
        logger.error('Configuration file does not exists, closing server!')
        sys.exit(0)
        
    
    def frames_to_time_format(self,total_frames):
        hours = total_frames // (3600 * 25) % 24
        minutes = (total_frames % (3600 * 25)) // (60 * 25)
        seconds = (total_frames % (60 * 25)) // 25
        frames = total_frames % 25
        return f"{hours:02}:{minutes:02}:{seconds:02}:{frames:02}"  

    def parse_time_to_frames(self,time_str):
        try:
            # Assuming time format is HH:mm:ss:ff
            hours, minutes, seconds, frames = map(int, time_str.split(':'))
            total_frames = (hours * 3600 + minutes * 60 + seconds) * 25 + frames
            return total_frames
        except ValueError:
            return 0



    def settings_provider(self):
        return {
            "status": "running",
            "playlist" : f"{self.global_context.get_value('current_recovered_playlist')}",
            "previous_running_id": f"{self.global_context.get_value('running_id')}",
            "previous_running_index": f"{self.global_context.get_value('actual_on_air_indice')}",
            "current_reckonkey": f"{self.global_context.get_value('current_reckonkey')}",
            "elapsed_frames":f"{self.global_context.get_value('elapsed_frames')}",
            "remaining_time":f"{self.global_context.get_value('remaining_time')}",
            "last_checked": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
        
    def get_current_playlist_by_guid(self):
        try:
            df = self.df.get_dataframe()
            guid=self.global_context.get_value('onair_guid')
            playlist_name = df.loc[df["GUID"] == guid, "playlist"].values[0]
            if playlist_name:
                return playlist_name
            else:
                print(f"[WARN] No matching GUID found: {guid}")
                self.logger.warning(f"No matching GUID found: {guid}")
                return None
        except Exception as e:
            print(f"[ERROR] Failed to get playlist by GUID: {e}")
            self.logger.error(f"Failed to get playlist by GUID: {e}")
            return None
        
    def start_asrun_thread(self):
        formatter=AsRunFormatter()
        asrun_thread= threading.Thread(target=formatter.run_asrun_formatter_periodically)
        asrun_thread.daemon = True 
        asrun_thread.start()

    def flush_remaining_asrun_before_shutdown(self):
   
        try:
            formatter=AsRunFormatter()
            current_date = date.today().strftime("%Y-%m-%d")
            asrun_file_path = os.path.join(
                self.global_context.get_value('asrun_location'),
                f"{self.global_context.get_value('channel_name')}-{current_date}"
            )
            self.logger.info(f"Flushing unwritten asrun data to file: {asrun_file_path}")
            formatter.write_to_file(asrun_file_path)
            formatter.send_batch_to_api()
            self.logger.info("Asrun data flush complete.")
        except Exception as flush_ex:
            self.logger.error(f"Error flushing remaining asrun data before shutdown: {flush_ex}", exc_info=True)
