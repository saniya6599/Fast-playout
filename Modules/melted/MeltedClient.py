import datetime
import json
import re
import socket
import os
import subprocess
import telnetlib
import threading
import time
import requests
import serial
import sys
import pandas as pd
from CommonServices.Global_context import GlobalContext
from CommonServices.DataFrameManager import DataFrameManager
from CommonServices.Time_management import timemgt
from CommonServices.config import Config
from CommonServices.Logger import Logger
from Modules.Graphics import Fastapi
from Modules.Graphics.FireTemplate import FireTemplate
from app.services.live_service import live_service
from app.services.scteservice import ScteService
from app.services.server_switch_service import server_switch_service
from Modules.Verify.PlaylistVerifier import PlaylistVerifier
from Modules.Verify.Database_verification import Database_verification
from app.Routes import Ts_switcher
from CommonServices.auto_recovery import AutoRecovery
from app.services import logo_service
import traceback
from CommonServices.reckonkey_generator import ReckonKeyGeneratorByPattern
from CommonServices.AsrunGenerator import AsRunFormatter

logger=Logger()
scte_service=ScteService()

class MeltedClient:
    def __init__(self):
        self.global_context = GlobalContext()
        self.melted_process = None
        self.server_ip = self.global_context.get_value("host")
        self.server_port = self.global_context.get_value('melted_port')
        self.melted_executable_path = self.global_context.get_value("melted_executable_path")
        self.connected = False
        self.client_socket = None
        self.update_thread = None
        self.append_thread =None
        self.live_thread=None
        self.update_thread_stop_event = threading.Event()
        self.append_thread_stop_event = threading.Event()
        self.update_complete_event = threading.Event()
        self.tn = None   #common terminal
        self.tn2=None    # usta status terminal
        self.tn3=None    # skip terminal

        self.db_verify = Database_verification()

        self.df_manager = DataFrameManager()
        # self.df_manager.get_dataframe()
        self.reckonkey=ReckonKeyGeneratorByPattern()

        self.on_air_guid = None
        self.next_guid=None
        global appended_guids
        appended_guids=[]

        global asrun_list
        asrun_list=[]
        self.global_context.set_value('asrun_list',asrun_list)
        # self.udp_ip=self.global_context.get_value('udp_packet_ip')
        # self.udp_port=int(self.global_context.get_value('udp_packet_port'))
        self.is_logo_overlayed = False
        self.is_busfile_triggered = False  # flag to track if the busfile is created
        self.is_break=False



        # Initialize the API client
        self.api = Fastapi.SPXGraphicsAPI(self.global_context.get_value("   "))
        self.render_logo= logo_service.logo_rendering_api(re.sub(r":\d+", ":8081", self.global_context.get_value("gfx_server")))
        self.recovery= AutoRecovery()

    def init_connection(self):
        try:
            logger.info(f"Trying to connect to {self.global_context.get_value('host')}:{self.server_port}")
            self.tn = telnetlib.Telnet(self.global_context.get_value("host"), self.server_port)
            self.tn2 = telnetlib.Telnet(self.global_context.get_value("host"),self.server_port)
            self.tn3 = telnetlib.Telnet(self.global_context.get_value("host"),self.server_port)
            self.connected = True
            return True
        except Exception as e:
            print(f"Failed to connect to playback server: {e}")
            logger.error(f"Failed to connect to playback server: {e}")
            return False

    def send_command(self, command):
     try:
        if not self.connected:
            print("Not connected to playback server. Trying to connect...")
            if not self.init_connection():
                return None

        self.tn.write(command.encode('ascii') + b"\n")
        response = self.tn.read_until(b"\n").decode('ascii')
        #print("Response from Melted server:", response)
        return response
     except Exception as e:
        print(f"Failed to send command {command}: {e}")
        return None

    def parse_usta(self, response):
     lines = response.split('\n')
     data_line_index = None

     # Find the index of the line that contains '202 OK'
    #  for i, line in enumerate(lines):
    #     if "202 OK" in line:
    #         data_line_index = i + 1
    #         break

     # If the line is found and there is a subsequent line
    #  if data_line_index is not None and data_line_index < len(lines):
     data = lines[0].split(' ')

        # Extract the relevant information
     status = data[1]  # 'playing'
     filename = data[2].strip('"')
     elapsed_frames = int(data[3])
     total_frames = int(data[7])
     onair_index = int(data[-1])

     return status, filename, elapsed_frames, total_frames, onair_index

    def start_update_thread(self):
        if self.update_thread and self.update_thread.is_alive():
            self.update_thread_stop_event.set()
            self.update_thread.join()

        self.update_thread_stop_event.clear()
        self.update_thread = threading.Thread(target=self.update_status_periodically)
        self.update_thread.start()

    def start_append_thread(self,df,guids,guid):
        if self.append_thread and self.append_thread.is_alive():
            self.append_thread_stop_event.set()
            self.append_thread.join()

        self.append_thread_stop_event.clear()
        self.append_thread = threading.Thread(target=self.append_remaining_inventory, args=(df, guids, guid))
        self.append_thread.start()

    def update_status_periodically(self):

         sec_guids = []
         previous_on_air_guid=None
         df = self.df_manager.get_dataframe()
         
         actual_on_air_indice = -1

         while not self.update_thread_stop_event.is_set():
            response =  self.send_usta_command("USTA U0")
            if response:
                status, filename, elapsed_frames, total_frames,onair_index = self.parse_usta(response)
                if status  == "playing":

                    current_on_air_guid = self.global_context.get_value("onair_guid")
                    df = self.df_manager.get_dataframe()
                    

                     # If the on-air GUID has changed
                    if previous_on_air_guid != current_on_air_guid:
                        #toggle up dataframe on id change
                        self.df_manager.toggle_up_dataframe()
                        self.global_context.set_value('sync',1)
                        
                        df = self.df_manager.get_dataframe()
                        
                        
                         #getting on air guid index for autorecovery
                        if previous_on_air_guid is not None:
                         self.global_context.set_value("onair_indice",df[df['GUID'] == previous_on_air_guid].index[0])
                        # #  shorten the df, only 10 previously played rows should be visible
                        #  if int(self.global_context.get_value("onair_indice")) > 10:
                        #   self.shortened_df(current_on_air_guid)
                        
                        
                        
                        # #updating status of previous on-air-guid to DONE
                        # df.loc[df['GUID'] == previous_on_air_guid, 'Status'] = 'DONE'
                       
                        on_air_id=df.loc[df['GUID'] == current_on_air_guid, 'Inventory'].values[0]
                        current_segment = df.loc[df['GUID'] == current_on_air_guid, 'Segment'].values[0]
                        Current_duration =  df.loc[df['GUID'] == current_on_air_guid, 'Duration'].values[0]
                        current_som = df.loc[df['GUID'] == current_on_air_guid, 'SOM'].values[0]
                        current_reckonkey = df.loc[df['GUID'] == current_on_air_guid, 'Reconcile'].values[0]
                        playlist = df.loc[df['GUID'] == current_on_air_guid, 'playlist'].values[0]
                    
                        
                        self.global_context.set_value('current_reckonkey',current_reckonkey)
                        
                        #on every id change increment the on air index

                        if previous_on_air_guid is not None and current_playlist != playlist :
                            logger.info(f"Now switched to playlist : {playlist}")
                            actual_on_air_indice =-1
                            
                        actual_on_air_indice = actual_on_air_indice+1
                        
                        
                        if(self.global_context.get_value('Is_server_in_recovery')):
                            actual_on_air_indice = self.global_context.get_value('recovery_index')
                            self.global_context.set_value('Is_server_in_recovery',False)
                            
                        self.global_context.set_value('actual_on_air_indice',actual_on_air_indice )       #Auto-recovery
                        
                        logger.info(f"NOW PLAYING : {on_air_id} {current_segment} {current_reckonkey} {current_som} {Current_duration}")

                        filename = os.path.splitext(os.path.basename(filename))[0]
                        if filename != on_air_id:
                            logger.critical("CONTENT MISMATCH!")
                            logger.critical(f"MISMATCH | SERVER : {filename} & PLAYOUT : {on_air_id}")
                            
                        if previous_on_air_guid is not None :
                            self.add_asrun_list(previous_on_air_guid)

                        #verifying full playlist on id change
                        if (self.global_context.get_value('auto_verification')):
                            print('Auto verification set to true, veryfying playlist..')
                            logger.info('Auto verification set to true, veryfying playlist..')

                            if not (self.global_context.get_value("verification_mode").startswith('Physical')):
                                self.db_verify.verify()
                            else :
                                verifier = PlaylistVerifier()
                                verifier.full_playlist_verification()
                                self.global_context.set_value('sync',1)
                            print(f'Full Playlist verification : completed on ID changed to {filename}')
                        else:
                            logger.info('Checking & update if new content downloaded..')
                            self.update_missing_verify()

                        # Update the previous on-air GUID
                        previous_on_air_guid = current_on_air_guid
                        current_playlist= df.loc[df['GUID'] == previous_on_air_guid, 'playlist'].values[0]
                        
                    
                        #fetching on air primary id index for checking and update missing content (status to be)
                        self.global_context.set_value("previous_on_air_primary_index", appended_guids.index(previous_on_air_guid))
                        
                        # self.global_context.set_value('actual_playlist_index', int(df.loc[df['GUID'] == previous_on_air_guid, 'playlist_index'].values[0]))
                        
                        csv_file_path = f"{self.global_context.get_value('busfile_location')}{self.global_context.get_value('channel_name')}.csv"
                        self.write_next_200_rows_to_csv(self.on_air_guid, csv_file_path)

                        sec_guids = self.collect_secondary_guids(df, previous_on_air_guid)

                        # Fetch the deatils of the on-air GUID
                        primary_event_time = df.loc[df['GUID'] == previous_on_air_guid, 'EventTime'].values[0]
                        primary_duration=df.loc[df['GUID'] == previous_on_air_guid, 'Duration'].values[0]
                        Inventory=df.loc[df['GUID'] == previous_on_air_guid, 'Inventory'].values[0]

                        #Logo hide on break
                        self.is_break = df.loc[df['GUID'] == previous_on_air_guid, 'Segment'].apply(lambda x: pd.isna(x) or x == "").values[0]
                        if self.is_break :
                         self.handle_logo_on_break()
                        elif not self.is_break and not self.is_logo_overlayed:
                         if self.render_logo.overlay("on"):
                                    print("started logo rendering, as segment after break detected!")
                                    logger.info("started logo rendering, as segment after break detected!")
                                    self.is_logo_overlayed = True

                        #SCTE usage only
                        # logger.info("Trigger FireTemplate on a separate thread")
                        threading.Thread(target=FireTemplate, args=(df,sec_guids, primary_event_time,primary_duration)).start()

                        # Call the new method to send the current running ID over UDP
                        # self.send_current_running_id_over_udp()

                        # csv_file_path = f"{self.global_context.get_value('busfile_location')}{self.global_context.get_value('channel_name')}.csv"
                        # self.write_next_200_rows_to_csv(self.on_air_guid, csv_file_path)

                        #changing event time to current time on Id change only
                        current_event_time = f"{timemgt.get_system_time_ltc()}"
                        event_time = current_event_time
                       

                    self.global_context.set_value("onair_index",onair_index)

                    if appended_guids:
                         self.on_air_guid = appended_guids[onair_index]


                    # event_row = df.loc[df['GUID'] == self.on_air_guid]
                    # if not event_row.empty:
                    #    event_time = event_row.iloc[0]['EventTime']

                    if onair_index == len(appended_guids) - 1:
                        self.next_guid = ""
                    else :
                        if appended_guids:
                         self.next_guid = appended_guids[onair_index+1]

                    self.global_context.set_value("automsg",status)
                    self.global_context.set_value("status", status)
                    self.global_context.set_value("running_id", filename)
                    self.global_context.set_value("elapsed_frames", elapsed_frames)
                    self.global_context.set_value("total_frames", total_frames)
                    self.global_context.set_value("elapsed_time", self.calculate_elapsed_time(elapsed_frames))
                    self.global_context.set_value("remaining_time", self.calculate_remaining_time(total_frames, elapsed_frames))
                    # self.global_context.set_value("channel_props", self.global_context.get_value("Playlist_loaded"))
                    self.global_context.set_value("onair_guid",self.on_air_guid)
                    self.global_context.set_value("ready_guid", self.next_guid)
                    self.global_context.set_value("event_time",event_time)

                if status == "paused" :
                    self.stop_melted()

            #time.sleep(1)

    def start_melted_server(self):
        try:
            if not self.init_connection():
                mode = self.global_context.get_value("OutMode")
                #config_path = f"{self.melted_executable_path}/udp_hdp.conf" if mode == '0' else f"{self.melted_executable_path}/ndi.conf" if mode == '1' else f"{self.melted_executable_path}/decklink.conf" if mode == '2' else None
                print(f"mode selected : {mode}")
                logger.info(f'mode selected : {mode}')
                config_path = f"{self.global_context.get_value('channel_name')}_udp_hdp.conf" if mode == 0 else "ndi.conf" if mode == 1 else "decklink.conf" if mode == 2 else None
                log_file_path = os.path.join(self.melted_executable_path,"melted_debug.log")
                # log_file_path="melted_debug.log"
                

                command = ["./melted", "-c", config_path, "-port", str(self.server_port)]
                # command = f"./melted -c {config_path} -test > {log_file_path} 2>&1"
                subprocess.run(command,check=True, cwd=self.melted_executable_path)
                
                print("Playback server started.")
                logger.info(f"Playback server started with configuration : {config_path}")
                return True
            return False
        except Exception as ex:
            print("Error starting playback server:", ex)
            logger.error("Error starting playback server:", ex)
            return False
        
        
    def start_melted_with_tmux(self):
        try:
            if not self.init_connection():
                mode = self.global_context.get_value("OutMode")
                config_path = f"{self.global_context.get_value('channel_name')}_udp_hdp.conf" if mode == 0 else "ndi.conf" if mode == 1 else "decklink.conf"
                session_name = f"melted_{self.server_port}"
                command = f"./melted -c {config_path} -port {self.server_port}"
                subprocess.run([
                    "tmux", "new-session", "-d", "-s", session_name,
                    "bash", "-c", command
                ], cwd=self.melted_executable_path, check=True)
                logger.info(f"Melted started in tmux session: {session_name}")
                return True

        except subprocess.CalledProcessError as e:
                logger.error(f"Failed to start melted in tmux: {e}")
                return False

        
        

    def check_executable(self):
        # Check if the directory exists
        if not os.path.isdir(self.melted_executable_path):
            print(f"Directory does not exist: {self.melted_executable_path}")
            return False

        # Check if the file exists
        executable_path = os.path.join(self.melted_executable_path, "start-melted-1")
        if not os.path.isfile(executable_path):
            print(f"Executable file does not exist: {executable_path}")
            return False

        # Check if the file has execute permissions
        file_stat = os.stat(executable_path)
        if not file_stat.st_mode & os.stat.S_IEXEC:
            print(f"File is not executable: {executable_path}")
            return False

        return True

    def send_inventory_commands(self, df_manager, GUID):
        try:
            
            if not (self.global_context.get_value("channel_props").startswith('recovery')) :
                df = self.df_manager.get_dataframe()
                index = df[df["GUID"]==GUID].index[0]
                self.df_manager.clear_dataframe()
                self.df_manager.load_dataframe(os.path.join(self.global_context.get_value("PlaylistLoc"), self.global_context.get_value("channel_props")),index)
                
                if not (self.global_context.get_value("verification_mode").startswith('Physical')):
                    logger.info("Database verification mode is on, Verifying before playout!")
                    self.db_verify.verify()
                    self.global_context.set_value('sync',1)
                else:
                    verifier = PlaylistVerifier()
                    verifier.full_playlist_verification()
                    self.global_context.set_value('sync',1)
            
            df = self.df_manager.get_dataframe()
            guids = df["GUID"]
            rows_count = len(df)
            logger.info(f"Rows count in loaded playlist : {rows_count}")
            self.global_context.set_value("rows_count", rows_count)
            first_row_guid = df.iloc[0]["GUID"] if rows_count > 0 else ""
            last_row_guid = df.iloc[-1]["GUID"] if rows_count > 0 else ""
            self.global_context.set_value("first_row_guid", first_row_guid)
            self.global_context.set_value("last_row_guid", last_row_guid)

            start_appending = False
            GUID= first_row_guid
            for guid in guids:
                if guid == GUID:
                    start_appending = True

                if start_appending:
                    appended_guids.append(guid)
                    inventory_item = df[df["GUID"] == guid]["Inventory"].iloc[0]
                    som = self.parse_time_to_frames(df[df["GUID"] == guid]["SOM"].iloc[0],)
                    eof = som + self.parse_time_to_frames(df[df["GUID"] == guid]["Duration"].iloc[0],)
                    # logger.info(f"Direct playing from ID : {inventory_item} som: {som}  and  eof {eof}")
                    file_extension = self.get_file_extension(inventory_item)
                    if(file_extension is None):
                            continue
                    command = f"APND U0 {inventory_item}{file_extension} {som} {eof}"
                    response = self.send_command(command)

                    if guid == GUID:

                        play_response = self.send_command("PLAY U0") 

                        if(self.render_logo.overlay("on")):
                         self.is_logo_overlayed = True
                        else:
                            logger.error("Failed to overlay logo on direct play!")

                        self.on_air_guid=guid
                        self.global_context.set_value("onair_guid",self.on_air_guid)

                        toggle_indice=df[df['GUID'] == guid].index[0]
                        self.df_manager.toggle_up_dataframe()
                        # self.global_context.set_value('sync',1)

                        #threading.Thread(target=self.append_remaining_inventory, args=(df, guids, guid)).start()
                        self.start_append_thread(df,guids,guid)
                        self.start_update_thread()
                        return f"{self.global_context.get_value('channel_name')}^_^DIRECT-PLAY^_^ACK"

            return f"{self.global_context.get_value('channel_name')}^_^DIRECT-PLAY^_^NACK"
        except Exception as ex:
            tb=traceback.extract_tb(ex.__traceback__)
            filename, line, func, text = tb[-1]  # Get the last traceback entry (where the error occurred)

            error_message = f"Error sending inventory commands: {ex} (File: {filename}, Function: {func}, Line: {line})"
            print(error_message)
            logger.error(error_message)  # exc_info=True logs the full traceback
            return f"{self.global_context.get_value('channel_name')}^_^DIRECT-PLAY^_^NACK"

    def send_inventory_commands_after_append(self, guids, GUID):
            try:
                #performing verification before appending
                if not (self.global_context.get_value("verification_mode").startswith('Physical')):
                    logger.info("performing database verification before appending new playlist")
                    self.db_verify.verify()
                else :
                    logger.info("performing physical verification before appending new playlist")
                    verifier = PlaylistVerifier()
                    verifier.full_playlist_verification()
                    
                self.global_context.set_value('sync',1)   
                df = self.df_manager.get_dataframe()
                start_appending = False

                for guid in guids:
                    if guid == GUID:
                        start_appending = True  

                    if start_appending:
                        # appended_guids.append(guid)
                        inventory_item = df[df["GUID"] == guid]["Inventory"].iloc[0]
                        som = self.parse_time_to_frames(df[df["GUID"] == guid]["SOM"].iloc[0],)
                        eof = som + self.parse_time_to_frames(df[df["GUID"] == guid]["Duration"].iloc[0],)
                        file_extension = self.get_file_extension(inventory_item)
                        file_extension = self.get_file_extension(inventory_item)
                        if(file_extension is None):
                            continue
                        
                        command = f"APND U0 {inventory_item}{file_extension} {som} {eof}"
                        response = self.send_command(command)
                        appended_guids.append(guid)


            except Exception as ex:
                print("Error sending inventory commands after append:", ex)

    def send_inventory_commands_afterlive(self, afterlive_guid):
        try:
            df = self.df_manager.get_dataframe()
            guids = df["GUID"]
            # rows_count = len(df)
            # logger.info(f"Rows count in loaded playlist : {rows_count}")
            # self.global_context.set_value("rows_count", rows_count)
            # first_row_guid = df.iloc[0]["GUID"] if rows_count > 0 else ""
            # last_row_guid = df.iloc[-1]["GUID"] if rows_count > 0 else ""
            # self.global_context.set_value("first_row_guid", first_row_guid)
            # self.global_context.set_value("last_row_guid", last_row_guid)

            live_start_appending = False

            for guid in guids:
                if guid == afterlive_guid:
                    live_start_appending = True

                if live_start_appending:
                    # appended_guids.append(guid)
                    next_inventory_item = df[df["GUID"] == guid]["Inventory"].iloc[0]
                    som = self.parse_time_to_frames(df[df["GUID"] == guid]["SOM"].iloc[0],)
                    eof = som + self.parse_time_to_frames(df[df["GUID"] == guid]["Duration"].iloc[0],)
                    logger.info(f"Next content ready to play after live : {next_inventory_item}")
                    print(f"Next content ready to play after live : {next_inventory_item}")
                    file_extension = self.get_file_extension(next_inventory_item)
                    if(file_extension is None):                                                             #changed here to avoid adding secondary ids
                            continue
                    command = f"APND U0 {next_inventory_item}{file_extension} {som} {eof}"
                    response = self.send_command(command)
                    appended_guids(guid)
                    break

        except Exception as ex:
            print("Error sending inventory commands:", ex)

    def append_remaining_inventory(self, df, guids, start_guid):

        # if self.append_thread_stop_event.is_set():
        #     logger.info("Append inventory thread stopped.")
        #     return

        while not self.append_thread_stop_event.is_set() :
            start_appending = False
            for guid in guids:
                if self.append_thread_stop_event.is_set():  # Double-check stop event inside loops
                    print("Stopping append inventory operation...")
                    break


                if guid == start_guid and guid in appended_guids:
                    continue  # Skip the first GUID as it was already appended
                else:
                    start_appending = True

                if start_appending:
                    event_type = df[df["GUID"] == guid]["Type"].iloc[0]
                    if event_type.lower() in ["pri", "primary"]:
                        inventory_item = df[df["GUID"] == guid]["Inventory"].iloc[0]
                        som = self.parse_time_to_frames(df[df["GUID"] == guid]["SOM"].iloc[0],)
                        eof = som + self.parse_time_to_frames(df[df["GUID"] == guid]["Duration"].iloc[0],)
                        # logger.info(f"Appending {inventory_item} som: {som}  and  eof {eof}")

                        # #checking if its live
                        # if "LIVE" in inventory_item:
                        #     appended_guids.append(guid)
                        # else :
                        file_extension = self.get_file_extension(inventory_item)
                        if(file_extension is None):
                            continue
                        command = f"APND U0 {inventory_item}{file_extension} {som} {eof}"
                        appended_guids.append(guid)
                        response = self.send_command(command)
                        time.sleep(0.2)
                        if response is None or "OK" not in response:
                            print(f"Failed to append inventory item {inventory_item} for GUID {guid}")
                #
                #
                # 6else:
                    # print(f"Skipping GUID {guid} as its Type is not 'PRI' ID")

            logger.info("Playlist loaded successfully into server!")
            self.append_thread_stop_event.set()

    def insert_guid(self,guid, index):
            appended_guids.insert(index, guid)
            print(appended_guids)

    def cut_paste_appended(self, source_index, target_index,new_guid):
        if 0 <= source_index < len(appended_guids) and 0 <= target_index < len(appended_guids):
            guid = appended_guids.pop(source_index)
            appended_guids.insert(target_index, new_guid)
            print(f"Moved GUID from index {source_index} to {target_index}: {appended_guids}")
        else:
            print("Invalid source or target index.")

    def start(self, GUID):
        try:
                if self.init_connection():
                        response = self.send_inventory_commands(self.df_manager, GUID)
                        return response
        except Exception as ex:
            logger.error(f"Error while starting : {ex}")
            print("Error while starting :", ex)

    def stop_update_thread(self):
        if self.update_thread and self.update_thread.is_alive():
            self.update_thread_stop_event.set()
            self.update_thread.join()

    def send_initial_commands(self):
        # commands = ["UADD sdl", "SET root=/home/pmsl/videos"]              #commented for now : this will work for sdl
        # #commands = ["SET root=/home/pmsl/videos"]
        # for command in commands:
        #     response = self.send_command(command)
        #     if "OK" not in response:
        #         print(f"Command {command} failed with response: {response}")
        #         #return False
        return True

    def bulk_delete_entries_by_guids(self, guids_str):
        try:
            
            if self.append_thread and self.append_thread.is_alive():
                logger.warning("Please wait for server to process append playlist!.")
                return f"{self.global_context.get_value('channel_name')}^_^BULK-DELETE^_^NACK^_^Please wait for server to process append playlist!"
         
            
            
            start_guid, end_guid = guids_str.split('~')
            df = self.df_manager.get_dataframe()
            df_primary = df[~df['Type'].str.lower().isin(['sec', 'secondary'])].reset_index(drop=True)

            # Find indices of start and end GUIDs in df_primary
            start_index = df[df["GUID"] == start_guid].index
            end_index = df[df["GUID"] == end_guid].index

            if start_index.empty or end_index.empty:
                logger.error(f"Start or end GUID not found: start={start_guid}, end={end_guid}")
                return f"{self.global_context.get_value('channel_name')}^_^DELETE^_^NACK^_^GUIDS_NOT_FOUND"

            start_index = start_index[0]
            end_index = end_index[0]

            if start_index > end_index:
                logger.warning("Start GUID appears after End GUID. Swapping.")
                start_index, end_index = end_index, start_index

            # All rows in range [start_index, end_index]
            guids_to_delete = df.iloc[start_index:end_index + 1]["GUID"].tolist()
            logger.info(f"Deleting range of GUIDs from {start_guid} to {end_guid}: {guids_to_delete}")

            for guid in guids_to_delete:
                status = df.loc[df["GUID"] == guid, "Status"].values[0]
                inventory_id = df.loc[df["GUID"] == guid, "Inventory"].values[0]

                if status == 'O.K':
                    # Get updated index in current df_primary
                   
                    current_primary = df[
                                        (~df['Type'].str.lower().isin(['sec', 'secondary'])) &
                                        (df['Status'].str.upper() != 'N.A')
                                    ].reset_index(drop=True)

                    index = current_primary[current_primary["GUID"] == guid].index
                    if not index.empty:
                        melted_index = index[0]
                        melted_command = f"REMOVE U0 {melted_index}"
                        try:
                            self.send_command(melted_command)
                            logger.info(f"Playback command executed: {melted_command}")
                        except Exception as melted_ex:
                            logger.error(f"Failed to execute melted command for {guid}: {melted_ex}")
                            return f"{self.global_context.get_value('channel_name')}^_^DELETE^_^NACK^_^ERROR"

                        if guid in appended_guids:
                            appended_guids.remove(guid)
                            logger.info(f"GUID {guid} removed from appended_guids.")

                # Remove from main dataframe
                df = df[df["GUID"] != guid]
                logger.info(f"GUID {guid} deleted from dataframe.")

            self.df_manager._df = df
            self.df_manager.toggle_up_dataframe()
            self.global_context.set_value("sync", 1)
            self.global_context.set_value("rows_count", len(df))

            return f"{self.global_context.get_value('channel_name')}^_^DELETE^_^{guids_str}^_^ACK"

        except Exception as ex:
            logger.critical(f"Critical error in bulk_delete_entries_by_guids: {ex}")
            return f"{self.global_context.get_value('channel_name')}^_^DELETE^_^NACK^_^ERROR"

    def delete_entries_by_guids(self, guids_str):
     try:
         
        if self.append_thread and self.append_thread.is_alive():
                logger.warning("Please wait for server to process append playlist!.")
                return f"{self.global_context.get_value('channel_name')}^_^DELETE^_^NACK^_^Please wait for server to process append playlist!"
         
        guids = guids_str.split('~')
        df = self.df_manager.get_dataframe()
        not_found_guids = []

        for guid in guids:
            
            status = df.loc[df["GUID"] == guid, "Status"].values[0]
            Eventtype=df.loc[df["GUID"] == guid, "Type"].values[0]

            if status == 'N.A' or Eventtype.lower() == 'sec':
                df = df[df["GUID"] != guid]
                print(f"GUID {guid} with status 'N.A' deleted from DataFrame only.")
                self.df_manager._df = df
                continue  # Skip Melted command
            
            
            if guid in df["GUID"].values:
               if guid in appended_guids:
                # Ensure the GUID exists before trying to get the index
                
                #  # Get playlist of the row to be deleted
                # playlist = df.loc[df["GUID"] == guid, "playlist"].values[0]

                
                
                guid_df = df[df["GUID"] == guid]
                if not guid_df.empty:
                    position = guid_df.index[0]

                df = df[df["GUID"] != guid]
                print(f"Entry with GUID {guid} deleted.")
            

                rows_count = len(df)
                self.global_context.set_value("rows_count", rows_count)
            else:
                not_found_guids.append(guid)
                print(f"GUID {guid} not found.")

            self.df_manager._df = df

            # Construct the Melted command to insert into the Melted unit
            deleteindex=appended_guids.index(guid)
            melted_command = f"REMOVE U0 {deleteindex}"
            try:
                    # Assuming you have a method to send commands to Melted, e.g., via TCP
                    self.send_command(melted_command)
                    print(f"Playback command executed: {melted_command}")
            except Exception as ex:
                    print(f"Error executing playback command: {ex}")
                    return f"{self.global_context.get_value('channel_name')}^_^DELETE^_^NACK^_^ERROR"

            # Remove the GUID from appended_guids if it's there
            if guid in appended_guids:
                appended_guids.remove(guid)
                print(f"GUID {guid} removed from appended_guids.")

        if not_found_guids:
            return f"{self.global_context.get_value('channel_name')}^_^DELETE^_^NACK^_^GUIDS_NOT_FOUND: {'~'.join(not_found_guids)}"
        else:
            self.df_manager.toggle_up_dataframe()
            self.global_context.set_value("sync",1)
            return f"{self.global_context.get_value('channel_name')}^_^DELETE^_^{guids_str}^_^ACK"
     except Exception as ex:
        print("Error deleting entries:", ex)
        return f"{self.global_context.get_value('channel_name')}^_^DELETE^_^NACK^_^ERROR"

    def content_insert_entry(self, command):
        try:
        
                     
            if self.append_thread and self.append_thread.is_alive():
                    logger.warning("Please wait for server to process append playlist!.")
                    return f"{self.global_context.get_value('channel_name')}^_^CONTENT-INSERT^_^NACK^_^Please wait for server to process append playlist!"
            
            
            parts = command.split("^_^")
            GUID = parts[2]
            all_entries = parts[3].split("<->") 
            response_guids = []
            
            df = self.df_manager.get_dataframe()
            dfposition = df[df["GUID"] == GUID].index[0]
            position = appended_guids.index(GUID)
            playlist=df.loc[df['GUID'] == GUID, 'playlist'].values[0]
            insert_datetime = df["DateTime"].mode()[0]
            verification_mode = self.global_context.get_value("verification_mode")


            for entry_str in all_entries:
                data = entry_str.split(",")
                filename = data[3]
                row_data = data[1:]

                new_guid = DataFrameManager.generate_guid()
                row_data[17] = new_guid

                file_extension = self.get_file_extension(filename)

                if str(filename).startswith("LIVE"):
                    print(f"ID to be inserted is {filename}")
                    row_data[11] = "RD"

                row_data.append(playlist)
                row_data[14] = insert_datetime
                used_keys = []  # can be expanded to track duplicates if needed
                row_data[6] = self.reckonkey.generate(used_keys)

                new_row = pd.DataFrame([row_data], columns=df.columns)
                df = pd.concat([df.iloc[:dfposition], new_row, df.iloc[dfposition:]]).reset_index(drop=True)
                self.df_manager._df = df
            
            
                #physical or db verification for inserted ID (SOM & duration in melted)
                if(self.global_context.get_value("verification_mode").startswith('Physical')):
                    print(f"Performing physical verification on inserted ID : {new_row['Inventory'][0]}")
                    verifier = PlaylistVerifier()
                    verifier.full_playlist_verification()
                    som = self.parse_time_to_frames(self.df_manager._df[self.df_manager._df["GUID"] == new_guid]["SOM"].iloc[0],)
                    eof = som + self.parse_time_to_frames(self.df_manager._df[self.df_manager._df["GUID"] == new_guid]["Duration"].iloc[0],)
                else :
                    print(f"Performing Database verification on inserted ID : {new_row['Inventory'][0]}")
                    self.db_verify.verify_content(new_row['Inventory'][0])
                    som = self.parse_time_to_frames(self.df_manager._df[self.df_manager._df["GUID"] == new_guid]["SOM"].iloc[0],)
                    eof = som + self.parse_time_to_frames(self.df_manager._df[self.df_manager._df["GUID"] == new_guid]["Duration"].iloc[0],)

                logger.info(f"Entry inserted at position {position} with GUID {new_guid}.")
            
                if file_extension is None :
                    #if non-existing Id has been inserted 
                    
                    response_guids.append(new_guid)                    
                    rows_count = len(self.df_manager._df)
                    self.global_context.set_value("rows_count", rows_count)

                    self.df_manager.toggle_up_dataframe()
                    self.global_context.set_value("sync",1)
                    continue
                    # return f"{self.global_context.get_value('channel_name')}^_^INSERT^_^{new_guid}^_^DONE"
                else:

                    melted_command = f"INSERT U0 {filename}{file_extension} {position} {som} {eof}"
                    try:
                        # Assuming you have a method to send commands to Melted, e.g., via TCP
                        self.send_command(melted_command)
                        print(f"Playback command executed: {melted_command}")
                    except Exception as melted_ex:
                        print(f"Error executing playback command: {melted_ex}")

                # Insert the new GUID into the appended_guids list at the specified index
                self.insert_guid(new_guid,position)
                response_guids.append(new_guid)
                dfposition += 1
                position += 1

            rows_count = len(df)
            self.global_context.set_value("rows_count", rows_count)
            self.df_manager.toggle_up_dataframe()
            self.global_context.set_value("sync", 1)

            return f"{self.global_context.get_value('channel_name')}^_^CONTENT-INSERT^_^{'~'.join(response_guids)}^_^DONE"

        except Exception as ex:
            logger.error("Error inserting entry:", ex)
            return f"{self.global_context.get_value('channel_name')}^_^CONTENT-INSERT^_^NACK^_^ERROR"
    
    def insert_entry(self, command):
        try:
            
            if self.append_thread and self.append_thread.is_alive():
                logger.warning("Please wait for server to process append playlist!.")
                return f"{self.global_context.get_value('channel_name')}^_^INSERT^_^NACK^_^Please wait for server to process append playlist!"
         
        
            
            parts = command.split("^_^")
            GUID = parts[2]
            data = parts[3].split(",")

            filename=data[3]
            
            df = self.df_manager.get_dataframe()
            dfposition = int(data[0]) - 1
             
            position = appended_guids.index(GUID)
            row_data = data[1:]

            new_guid = DataFrameManager.generate_guid()
            row_data[17] = new_guid
            
            
            file_extension = self.get_file_extension(filename)
            
            

            if(str(filename).startswith("LIVE")):
                print(f"ID to be inserted is {filename}")
                row_data[11]="RD"

            
            
            playlist=df.loc[df['GUID'] == GUID, 'playlist'].values[0]
            # target_playlist_index = df.loc[df['GUID'] == GUID, 'playlist_index'].values[0]
            # new_playlist_index = max(int(target_playlist_index) - 1, 0)  
            row_data.append(playlist)
            # row_data.append(new_playlist_index) 

            #inserting date
            row_data[14]=df["DateTime"].mode()[0]
            
            #generate and give new reckon key
            used_keys=[]
            row_data[6] = self.reckonkey.generate(used_keys)

            new_row = pd.DataFrame([row_data], columns=df.columns)

            
            self.df_manager._df = pd.concat([df.iloc[:dfposition], new_row, df.iloc[dfposition:]]).reset_index(drop=True)
            
            #Recalculate playlist_index only for the affected playlist
            # df = self.df_manager._df
            # mask = df['playlist'] == playlist
            # df.loc[mask, 'playlist_index'] = range(mask.sum())
            # self.df_manager._df=df
            
            
            #physical or db verification for inserted ID (SOM & duration in melted)
            if(self.global_context.get_value("verification_mode").startswith('Physical')):
                print(f"Performing physical verification on inserted ID : {new_row['Inventory'][0]}")
                verifier = PlaylistVerifier()
                verifier.full_playlist_verification()
                som = self.parse_time_to_frames(self.df_manager._df[self.df_manager._df["GUID"] == new_guid]["SOM"].iloc[0],)
                eof = som + self.parse_time_to_frames(self.df_manager._df[self.df_manager._df["GUID"] == new_guid]["Duration"].iloc[0],)
            else :
                print(f"Performing Database verification on inserted ID : {new_row['Inventory'][0]}")
                self.db_verify.verify_content(new_row['Inventory'][0])
                som = self.parse_time_to_frames(self.df_manager._df[self.df_manager._df["GUID"] == new_guid]["SOM"].iloc[0],)
                eof = som + self.parse_time_to_frames(self.df_manager._df[self.df_manager._df["GUID"] == new_guid]["Duration"].iloc[0],)

            logger.info(f"Entry inserted at position {position} with GUID {new_guid}.")
            
            if file_extension is None :
                #if non-existing Id has been inserted 
                rows_count = len(self.df_manager._df)
                self.global_context.set_value("rows_count", rows_count)

                self.df_manager.toggle_up_dataframe()
                self.global_context.set_value("sync",1)
                return f"{self.global_context.get_value('channel_name')}^_^INSERT^_^{new_guid}^_^DONE"


            if(str(filename).startswith("LIVE")):
                print(f"Live Id recieved to insert, directly inserting in list.")
            else:
                # Construct the Melted command to insert into the Melted unit
                melted_command = f"INSERT U0 {filename}{file_extension} {position} {som} {eof}"
                try:
                    # Assuming you have a method to send commands to Melted, e.g., via TCP
                    self.send_command(melted_command)
                    print(f"Playback command executed: {melted_command}")
                except Exception as melted_ex:
                    print(f"Error executing playback command: {melted_ex}")

            # Insert the new GUID into the appended_guids list at the specified index
            self.insert_guid(new_guid,position)

            rows_count = len(self.df_manager._df)
            self.global_context.set_value("rows_count", rows_count)

            self.df_manager.toggle_up_dataframe()
            self.global_context.set_value("sync",1)
            return f"{self.global_context.get_value('channel_name')}^_^INSERT^_^{new_guid}^_^DONE"
        except Exception as ex:
            logger.error("Error inserting entry:", ex)
            return f"{self.global_context.get_value('channel_name')}^_^INSERT^_^NACK^_^ERROR"
    
    # def update_missing_verify(self):
    #         try:
    #             lines = []
                
    #             df = self.df_manager.get_dataframe()
    #             print(appended_guids)
    #             if df is None or df.empty:
    #                 return lines

    #             filtered_df = df[
    #                 df['Type'].str.lower().isin(['pri', 'primary']) & 
    #                 ~df['Inventory'].str.startswith('LIVE', na=False)& 
    #                 (df['Status'] == 'N.A')
    #             ]
    #             filtered_df = filtered_df.reset_index(drop=True)   #comment this also
                
    #             unique_inventory_ids = filtered_df["Inventory"].unique()
    #             verified_ids = []
    #             # self.db_verify.verify()  # Perform DB verification

    #             for inventory_id in unique_inventory_ids:
    #                 clip_ext= self.get_file_extension(inventory_id)
    #                 clip_path = f"{os.path.join(self.global_context.get_value('ContentLoc'), inventory_id)}{clip_ext}"

    #                 if os.path.exists(clip_path):  
    #                     # If clip exists, update status to "O.K"
    #                     row_indexes_server = filtered_df[filtered_df['Inventory'] == inventory_id].index   #melted index   get guids from appended guids list for actual index in melted to insert
    #                     row_indexes = df[df['Inventory'] == inventory_id].index                             #playlist index   //COmment this then
    #                     for row_index in row_indexes:
                        
    #                         if df.at[row_index, 'Status'] == "N.A":
    #                             self.df_manager.update_row(row_index, 'Status', "O.K")
    #                             verified_ids.append(row_index)       
    #                 else : 
    #                     row_indexes = df[df['Inventory'] == inventory_id].index
    #                     for row_index in row_indexes:
    #                         self.df_manager.update_row(row_index, 'Status', "N.A")

    #             # Run database verification on verified IDs
    #             if verified_ids:
    #                 self.db_verify.verify()  # Perform DB verification
    #                 self.insert_update_missing_into_melted(verified_ids)
    #                 logger.info(f"verified_ids after update missing are : {verified_ids}")

    #                 # # Insert verified IDs into Melted
                

    #             # return self.df_manager.get_lines()  # Return updated playlist lines
    #             self.df_manager.toggle_up_dataframe()
    #             self.global_context.set_value('sync',1)

    #         except Exception as e:
    #             print(f"Error in Update missing : {e}")
    #             logger.error(f"Error in Update missing :{e}")

    def update_missing_verify(self):
       try:
           
        df = self.df_manager.get_dataframe()

        df_primary = df[~df['Type'].str.lower().isin(['sec', 'secondary'])].reset_index(drop=True)
        
        #unique after onair only
        onair_indice=self.global_context.get_value('previous_on_air_primary_index')
       
        unique_inventory_ids= df.loc[(df.index > (onair_indice if onair_indice not in (None, '') else -1)) & ~df["Type"].str.lower().isin(['sec', 'secondary']), "Inventory"].unique()

        verified_ids = []  

        content_loc = self.global_context.get_value('ContentLoc')

        for inventory_id in unique_inventory_ids:

          df = self.df_manager.get_dataframe()
          filtered_df = df[
            (df['Type'].str.lower().isin(['pri', 'primary'])) &
            (~df['Inventory'].str.startswith('LIVE', na=False)) &
            (df['Status'] == 'N.A')   #comment this
        ]
        
          filtered_df_ok = df[
            (df['Type'].str.lower().isin(['pri', 'primary'])) &
            (~df['Inventory'].str.startswith('LIVE', na=False)) &
            (df['Status'] == 'O.K')
        ].reset_index(drop=True)

          df_primary = df[~df['Type'].str.lower().isin(['sec', 'secondary'])].reset_index(drop=True)
            
          try:
               
            clip_ext = self.get_file_extension(inventory_id)
            clip_path = os.path.join(content_loc, f"{inventory_id}{clip_ext}")
                                    
            if self.is_file_stable(clip_path):
                
                if any(filtered_df.loc[filtered_df['Inventory'] == inventory_id, 'Status'] == 'N.A'):
                    df.loc[df['Inventory'] == inventory_id, 'Status'] = 'O.K'
                    df_primary.loc[df_primary['Inventory'] == inventory_id, 'Status'] = 'O.K'
                    self.db_verify.verify_content(inventory_id)


                    updated =  df_primary[df_primary['Status'] != 'N.A'].reset_index(drop=True)
                    verified_indexes = updated[updated['Inventory']==inventory_id].index.tolist()
                    # verified_indexes = df_primary[df_primary['Inventory'] == inventory_id].index.tolist()
                    # verified_indexes = df_primary[
                    #                 (df_primary['Status'].str.upper() != 'N.A') & (df_primary['Inventory'] == inventory_id)
                    #             ].reset_index(drop=True).index.tolist()
                                                                
                    
                    self.insert_update_missing_into_melted(verified_indexes,updated)
                    # logger.info(f"Verified IDs after update missing: {verified_ids}")
                    self.df_manager.toggle_up_dataframe()
                    self.global_context.set_value('sync', 1)
                    # verified_ids.extend(verified_indexes)
                    logger.info(f"Updated status to O.K for : {inventory_id}")
                    break
                             
            else : 
                    all_indexes = df_primary[df_primary['Inventory'] == inventory_id].index
                    valid_indexes = [
                        idx for idx in all_indexes
                        if idx > onair_indice and df_primary.at[idx, 'Status'] == 'O.K'
                    ]

                    if valid_indexes:
                        logger.warning(f"File : {inventory_id} was marked O.K but is now missing. Marking as N.A.")
                        self.remove_missing_from_melted(valid_indexes)
                        
                        df_matches = df[
                        (df['Inventory'] == inventory_id) &
                        (df['Status'] == 'O.K') &
                        (df.index > onair_indice)
                        ]
                        
                        for idx in df_matches.index:
                            self.df_manager.update_row(idx, 'Status', 'N.A')
                            
                # #for ids that are present during appending procedure but gets deleted/missing
                # if any(df_primary.loc[df_primary['Inventory'] == inventory_id, 'Status'] == 'O.K'):
                #     logger.warning(f"File : {inventory_id} was marked O.K but is now missing or unstable. Marking as N.A.")
                   
                #    ##Commenting this for correct insert
                   
                #     # # indexes = df_primary[df_primary['Inventory'] == inventory_id].index  
                #     # indexes=filtered_df_ok[(filtered_df_ok['Inventory'] == inventory_id)].index
                #     # self.remove_missing_from_melted(indexes)
                #     # df.loc[df['Inventory'] == inventory_id, 'Status'] = 'N.A'
                    
                    
                
                #for ids which where missing from the start and are still misiing/downloading   : not required though
                    row_indexes = filtered_df[filtered_df['Inventory'] == inventory_id].index  
                    for row_index in row_indexes:
                        self.df_manager.update_row(row_index, 'Status', "N.A")
                    logger.warning(f"File not found/still downloading for inventory ID: {inventory_id}, keeping status as N.A")
          except Exception as file_check_ex:
               logger.error(f"Error checking file existence for inventory ID {inventory_id}: {file_check_ex}")

                    
        # Run database verification on verified IDs
        # if verified_ids:
        #         try:
                   
        #             self.insert_update_missing_into_melted(verified_ids)
        #             logger.info(f"Verified IDs after update missing: {verified_ids}")
        #             self.df_manager.toggle_up_dataframe()
        #             self.global_context.set_value('sync', 1)
        #         except Exception as db_ex:
        #             logger.critical(f"Database verification or server update failed: {db_ex}")
       except Exception as ex:
            logger.critical(f"Critical error in update_missing_verify: {ex}")
            
    def get_file_extension(self, filename):
      for root, _, files in os.walk(self.global_context.get_value("ContentLoc")):
            for file in files:
                if file.startswith(filename) and file[len(filename):len(filename)+1] == ".":
                    return os.path.splitext(file)[1]
      return None

    def insert_update_missing_into_melted(self,verifiedids,df_primary):
        try:
           print(appended_guids)
           logger.info(f"Content downloaded & ready to insert : {verifiedids}")
           df = self.df_manager.get_dataframe()
           
           #onair_indice = self.global_context.get_value("onair_indice")
           #get primary on air indice
           onair_indice = self.global_context.get_value("previous_on_air_primary_index")
           
           
        #    filtered_df = df[
        #         df['Type'].str.lower().isin(['pri', 'primary']) &
        #         ~df['Inventory'].str.startswith('LIVE', na=False)
        #     ]
        #    guids =  filtered_df['GUID']


           
        #    df_primary = df[~df['Type'].str.lower().isin(['sec', 'secondary'])].reset_index(drop=True)
           
           row_indexs = [idx for idx in verifiedids if idx > onair_indice]
           logger.info(f"On Air Indice : {onair_indice}")

           for idx in row_indexs:    # reverse inserting in order to manage indexes
              try:
                row = df_primary.iloc[idx]
                guid =row['GUID']
                Inventory= row['Inventory']
                position = idx
                file_extension = self.get_file_extension(Inventory)
                som = self.parse_time_to_frames(df[df["GUID"] == guid]["SOM"].iloc[0],)
                eof = som + self.parse_time_to_frames(df[df["GUID"] == guid]["Duration"].iloc[0],)
                
                # logger.info(f"Inserting into server : {Inventory}") 


                # Construct the Melted command to insert into the Melted unit
                melted_command = f"INSERT U0 {Inventory}{file_extension} {position} {som} {eof}"
                try:
                        # Assuming you have a method to send commands to Melted, e.g., via TCP
                        self.send_command(melted_command)
                        logger.info(f"Playback command executed: {melted_command}")
                        print(f"Playback command executed: {melted_command}")
                except Exception as melted_ex:
                        logger.error(f"Error executing playback command: {melted_ex}")

                # Insert the new GUID into the appended_guids list at the specified index
                self.insert_guid(guid,position)

                rows_count = len(self.df_manager._df)
                self.global_context.set_value("rows_count", rows_count)

                self.df_manager.toggle_up_dataframe()
                self.global_context.set_value("sync",1)
              except Exception as idx_ex:
                    logger.error(f"Error processing index {idx}: {idx_ex} in Insert_update_missing_into_melted")
        except Exception as ex:
                print("Error inserting entry:", ex)
                return f"{self.global_context.get_value('channel_name')}^_^INSERT^_^NACK^_^ERROR"

    def is_file_stable(self,path, wait_time=0.5):

        try:
            # logger.info(f"Clip path : {path}")
            if not os.path.exists(path):
                return False

            size_before = os.path.getsize(path)
            print(f"Initial size of file '{path}': {size_before} bytes")
            
            time.sleep(wait_time)

            size_after = os.path.getsize(path)
            print(f"Size after {wait_time} seconds: {size_after} bytes")

            if size_before == size_after:
                # logger.info(f"{os.path.basename(clip_path)} size is stable. File is likely fully downloaded.")
                print(f"{os.path.basename(path)} size is stable. File is likely fully downloaded.")
                return True
            else:
                logger.info(f"{os.path.basename(path)} may still be downloading")
                return False

        except Exception as e:
            logger.error(f"Error while checking content stability : {e}")
            return False

    
    def remove_missing_from_melted(self,indexes):
    
            try:    
                    logger.info(f"Removing indexes from server : {indexes}")
                    for index in sorted(indexes, reverse=True):
                        # Construct the Melted command to remove mising ids from list
                        melted_command = f"REMOVE U0 {index}"
                        try:
                                result=self.send_command(melted_command)
                                # logger.info(f"result from melted : {result}")
                                logger.info(f"Playback command executed: {melted_command}")
                        except Exception as melted_ex:
                                logger.error(f"Error executing playback command: {melted_ex}")

                    # REMOVE new GUID into the appended_guids list at the specified index
                    # global appended_guids
                    appended_guids[:] = [item for i, item in enumerate(appended_guids) if i not in indexes]


                    # rows_count = len(self.df_manager._df)
                    # self.global_context.set_value("rows_count", rows_count)

                    self.df_manager.toggle_up_dataframe()
                    self.global_context.set_value("sync",1)

            except Exception as e:
                logger.error(f"Error communicating with Melted: {e}")

    def calculate_elapsed_time(self, elapsed_frames, frame_rate=25):
        elapsed_seconds = elapsed_frames / frame_rate
        hours = int(elapsed_seconds // 3600)
        minutes = int((elapsed_seconds % 3600) // 60)
        seconds = int(elapsed_seconds % 60)
        frames = int((elapsed_seconds - int(elapsed_seconds)) * frame_rate)

        # Format the time as HH:mm:ss:ff
        elapsed_time = f"{hours:02}:{minutes:02}:{seconds:02}:{frames:02}"
        return elapsed_time

    def calculate_remaining_time(self, total_frames, elapsed_frames, frame_rate=25):
        remaining_frames = total_frames - elapsed_frames
        remaining_seconds = remaining_frames / frame_rate
        hours = int(remaining_seconds // 3600)
        minutes = int((remaining_seconds % 3600) // 60)
        seconds = int(remaining_seconds % 60)
        frames = int((remaining_seconds - int(remaining_seconds)) * frame_rate)

        # Format the time as HH:mm:ss:ff
        remaining_time = f"{hours:02}:{minutes:02}:{seconds:02}:{frames:02}"
        return remaining_time

    # def cut_paste(self, command):
    #     df_manager = DataFrameManager()
    #     df = df_manager.get_dataframe()
    #     try:
    #         parts = command.split('^_^')
    #         if len(parts) < 5:
    #                 return "Invalid command format"

    #         cut_ids = parts[3].split('~')
    #         paste_above_id = parts[2]
    #         paste_above_index = df[df['GUID'] == paste_above_id].index[0]-1


    #         for cut_id in cut_ids :
    #             sourceindex_Appended=appended_guids.index(cut_id)
    #             targetindex_Apeended=appended_guids.index(paste_above_id)



    #             if cut_id not in df['GUID'].values or paste_above_id not in df['GUID'].values:
    #                 return "One or both GUIDs not found in the DataFrame"

    #             # Get the index positions of the cut_id and paste_above_id
    #             cut_index = df[df['GUID'] == cut_id].index[0]



    #             target_index_mlt=appended_guids.index(paste_above_id)


    #             # Extract the row to be cut
    #             cut_row = df.loc[cut_index].copy()

    #             # Generate a new GUID for the cut row
    #             new_guid = DataFrameManager.generate_guid()
    #             cut_row['GUID'] = new_guid

    #             # Drop the row from the DataFrame
    #             df = df.drop(cut_index)

    #             # Adjust paste_above_index if necessary
    #             if paste_above_index > cut_index:
    #                 paste_above_index -= 1  # Adjust index if paste is below cut

    #             # Insert the row before the specified row (pasting over the destination)
    #             df = pd.concat([df.iloc[:paste_above_index], pd.DataFrame([cut_row]), df.iloc[paste_above_index:]]).reset_index(drop=True)

    #             # Update the DataFrame in the DataFrameManager instance
    #             df_manager._df = df


    #             # Construct and send the MOVE command to Melted
    #             move_command = f"MOVE U0 {sourceindex_Appended} {targetindex_Apeended}"
    #             try:
    #                     self.send_command(move_command)
    #                     print(f"Melted MOVE command executed: {move_command}")
    #             except Exception as melted_ex:
    #                     print(f"Error executing Melted MOVE command: {melted_ex}")

    #             self.cut_paste_appended(sourceindex_Appended,targetindex_Apeended,new_guid)


    #             self.df_manager.toggle_up_dataframe()
    #             self.global_context.set_value('sync',1)
    #             return f"MLT^_^CUT-PASTE^_^{cut_id}^_^{paste_above_id}"

    #     except Exception as e:
    #         print(f"[ERROR] Error in action CUT-PASTE: {e}")
    #         logger.error("Error in action CUT-PASTE:")

    # def cut_paste(self, command):
    #     df_manager = DataFrameManager()
    #     df = df_manager.get_dataframe()
    #     try:
    #         parts = command.split('^_^')
    #         if len(parts) < 5:
    #             return "Invalid command format"

    #         cut_ids = parts[3].split('~')  # List of GUIDs to cut
    #         paste_above_id = parts[2]  # Target GUID
    #         paste_above_index = df[df['GUID'] == paste_above_id].index[0]  # Correct index for pasting

    #         # Check if all cut_ids exist
    #         if not all(cut_id in df['GUID'].values for cut_id in cut_ids) or paste_above_id not in df['GUID'].values:
    #             return "One or more GUIDs not found in DataFrame"

    #         cut_rows = df[df['GUID'].isin(cut_ids)]  # Extract all rows to be moved
    #         cut_indices = df[df['GUID'].isin(cut_ids)].index.tolist()  # Get their indices

    #         # Remove rows from DataFrame
    #         df = df.drop(cut_indices).reset_index(drop=True)

    #         # Adjust paste_above_index if necessary
    #         if paste_above_index > max(cut_indices):
    #             paste_above_index -= len(cut_ids)  # Shift paste index if cutting from above

    #         # Insert rows back at the correct position
    #         df = pd.concat([df.iloc[:paste_above_index], cut_rows, df.iloc[paste_above_index:]]).reset_index(drop=True)

    #         # Update DataFrame
    #         df_manager._df = df

    #         # Update appended_guids list
    #         for cut_id in reversed(cut_ids):  # Maintain relative order
    #             appended_guids.remove(cut_id)
    #         insert_pos = appended_guids.index(paste_above_id)  # Get correct insertion index
    #         for cut_id in reversed(cut_ids):  # Insert in order
    #             appended_guids.insert(insert_pos, cut_id)

    #         # Construct and send MOVE commands for each cut_id
    #         for cut_id in cut_ids:
    #             source_index = appended_guids.index(cut_id)
    #             target_index = appended_guids.index(paste_above_id)
    #             move_command = f"MOVE U0 {source_index} {target_index}"
    #             try:
    #                 self.send_command(move_command)
    #                 print(f"Melted MOVE command executed: {move_command}")
    #             except Exception as melted_ex:
    #                 print(f"Error executing Melted MOVE command: {melted_ex}")
    #             # self.cut_paste_appended(source_index,target_index,cut_id)

    #         self.df_manager.toggle_up_dataframe()
    #         self.global_context.set_value('sync', 1)

    #         return f"MLT^_^CUT-PASTE^_^{','.join(cut_ids)}^_^{paste_above_id}"

    #     except Exception as e:
    #         print(f"[ERROR] Error in action CUT-PASTE: {e}")
    #         logger.error("Error in action CUT-PASTE:")

    def cut_paste(self, command):
        df_manager = DataFrameManager()
        df = df_manager.get_dataframe()
        try:
        
                     
            if self.append_thread and self.append_thread.is_alive():
                logger.warning("Please wait for server to process append playlist!.")
                return f"{self.global_context.get_value('channel_name')}^_^CUT-PASTE^_^NACK^_^Please wait for server to process append playlist!"
         
        
            
            parts = command.split('^_^')
            if len(parts) < 5:
                return "Invalid command format"

            cut_ids = parts[3].split('~')
            paste_above_id = parts[2]

            if not all(cut_id in df['GUID'].values for cut_id in cut_ids) or paste_above_id not in df['GUID'].values:
                logger.error("One or more GUIDs not found in DataFrame")


            # Extract and remove cut rows
            # cut_rows = df[df['GUID'].isin(cut_ids)].copy()
            #cut_indices = df[df['GUID'].isin(cut_ids)].index.tolist()
            # df = df.drop(cut_indices).reset_index(drop=True)
            
            # 1. Remove cut rows from df first
            cut_rows = df[df['GUID'].isin(cut_ids)].copy()
            cut_indices = df[df['GUID'].isin(cut_ids)].index.tolist()
            df = df[~df['GUID'].isin(cut_ids)].reset_index(drop=True)

            # Find the correct paste position
            paste_above_index = df[df['GUID'] == paste_above_id].index[0]
            if paste_above_index > max(cut_indices):
                paste_above_index -= len(cut_ids)

            # Insert cut rows at the correct position
            df = pd.concat([df.iloc[:paste_above_index], cut_rows, df.iloc[paste_above_index:]]).reset_index(drop=True)
            df_manager._df = df  # Update DataFrameManager


            for cut_id in cut_ids:
                source_index = appended_guids.index(cut_id)
                target_index = appended_guids.index(paste_above_id)

                move_command = f"MOVE U0 {source_index} {target_index}"
                try:
                    self.send_command(move_command)
                    print(f"Playback MOVE command executed: {move_command}")
                except Exception as melted_ex:
                    print(f"Error executing MOVE command: {melted_ex}")

                # Updated cut_paste_appended logic
                if 0 <= source_index < len(appended_guids) and 0 <= target_index < len(appended_guids):
                    guid = appended_guids.pop(source_index)
                    appended_guids.insert(target_index, guid)
                    print(f"Moved GUID from index {source_index} to {target_index}: {appended_guids}")
                else:
                    print("Invalid source or target index.")


            # Remove cut IDs from appended_guids
            for cut_id in reversed(cut_ids):
                appended_guids.remove(cut_id)

            # Find target position in appended_guids and insert cut IDs in order
            target_index = appended_guids.index(paste_above_id)
            for cut_id in reversed(cut_ids):
                appended_guids.insert(target_index, cut_id)
                

            # Process MOVE commands
            self.df_manager.toggle_up_dataframe()
            self.global_context.set_value('sync', 1)

            return f"{self.global_context.get_value('channel_name')}^_^CUT-PASTE^_^{','.join(cut_ids)}^_^{paste_above_id}"

        except Exception as e:
            print(f"[ERROR] Error in action CUT-PASTE: {e}")
            logger.error("Error in action CUT-PASTE:")

    def paste_entries(self, command):
     try:
         
        if self.append_thread and self.append_thread.is_alive():
                logger.warning("Please wait for server to process append playlist!.")
                return f"{self.global_context.get_value('channel_name')}^_^PASTE^_^NACK^_^Please wait for server to process append playlist!"
         
         
         
         
        # Parse the command to extract GUIDs
        parts = command.split("^_^")
        target_guid = parts[2]
        source_guids_str = parts[3]
        source_guids = source_guids_str.split('~')

        # Create a DataFrameManager instance and get the DataFrame
        df_manager = DataFrameManager()
        df = df_manager.get_dataframe()

        # Find the position to insert the new rows using target_guid
        target_row = df.loc[df["GUID"] == target_guid]
        if target_row.empty:
            return f"{self.global_context.get_value('channel_name')}^_^PASTE^_^{source_guids_str}^_^{target_guid}^_^NACK^_^ERROR: Target row not found"

        target_index = target_row.index[0]
        target_index_mlt=appended_guids.index(target_guid)

        new_rows = []
        new_guids = []
        used_keys = df["Reconcile"].dropna().astype(str).tolist()
        
        for source_guid in source_guids:
            # Find the row to copy using source_guid
            source_row = df.loc[df["GUID"] == source_guid]
            source_index=source_row.index[0]
            source_index_mlt=appended_guids.index(source_guid)

            if not source_row.empty:
                source_row_data = source_row.iloc[0].astype(str).tolist()
                new_reckonkey = self.reckonkey.generate(used_keys)
                used_keys.append(new_reckonkey)
                source_row_data[6]=new_reckonkey     #generating new unique reckon key for pasted ids

                # Generate a new GUID for the new row
                new_guid = DataFrameManager.generate_guid()
                source_row_data[17] = new_guid  # Assuming the GUID column is at index 17
                filename=source_row_data[2]
                file_extension=source_row_data[13]
                
                som= self.parse_time_to_frames(source_row_data[7])
                eof= som + self.parse_time_to_frames(source_row_data[8])

                # Create the new row DataFrame
                new_row_df = pd.DataFrame([source_row_data], columns=df.columns)
                new_rows.append(new_row_df)
                new_guids.append(new_guid)

                print(f"Row copied from GUID {source_guid} to new GUID {new_guid} above GUID {target_guid}.")
            else:
                print(f"Source row with GUID {source_guid} not found.")


              # Construct and send the MOVE command to Melted
            # move_command = f"MOVE U0 {source_index_mlt} {target_index_mlt}"

            # Construct the Melted command to insert into the Melted unit
            move_command = f"INSERT U0 {filename}{file_extension} {target_index_mlt} {som} {eof}"

            try:
                self.send_command(move_command)
                print(f" MOVE command executed: {move_command}")
            except Exception as melted_ex:
                print(f"Error executing MOVE command: {melted_ex}")

            # Insert the new GUID into the appended_guids list at the specified index
            self.insert_guid(new_guid,target_index_mlt)
            target_index_mlt+=1

        if new_rows:
            # Insert all new rows just above the target row
            df = pd.concat(
                [df.iloc[:target_index], *new_rows, df.iloc[target_index:]]
            ).reset_index(drop=True)

            # Update the DataFrame in the DataFrameManager instance
            df_manager._df = df

            self.df_manager.toggle_up_dataframe()
            self.global_context.set_value("sync",1)
            
            return f"{self.global_context.get_value('channel_name')}^_^PASTE^_^{source_guids_str}^_^{target_guid}^_^ACK^_^{','.join(new_guids)}"
        else:
            return f"{self.global_context.get_value('channel_name')}^_^PASTE^_^{source_guids_str}^_^{target_guid}^_^NACK^_^ERROR: Source rows not found"
     except Exception as ex:
        print("Error pasting entries:", ex)
        return f"{self.global_context.get_value('channel_name')}^_^PASTE^_^{source_guids_str}^_^{target_guid}^_^NACK^_^ERROR"

    def verify_entry(self, command):
        try:
            # Parse the command to extract the GUID
            parts = command.split("^_^")
            guid = parts[2]

            # Create a DataFrameManager instance and get the DataFrame
            df_manager = DataFrameManager()
            df = df_manager.get_dataframe()

            # Find the row with the specified GUID
            row = df.loc[df["GUID"] == guid]

            if not row.empty:
                index = row.index[0]
                row_data = row.iloc[0].astype(str).tolist()
                row_str = f"{index+1}," + ",".join(row_data)
                return f"{self.global_context.get_value('channel_name')}^_^VERIFY^_^{guid}^_^{row_str}"
            else:
                return f"{self.global_context.get_value('channel_name')}^_^VERIFY^_^{guid}^_^NACK^_^ERROR: Row not found"
        except Exception as ex:
            print("Error verifying entry:", ex)
            return f"{self.global_context.get_value('channel_name')}^_^VERIFY^_^{guid}^_^NACK^_^ERROR"

    def send_usta_command(self, command):
        try:
            if not self.connected:
                print("Not connected to playback server. Trying to connect...")
                if not self.init_connection():
                    return None

            self.tn2.write(command.encode('ascii') + b"\n")

            # Read response from the server
            response = self._read_response()
            #print("Response from Melted server:", response)
            return response
        except Exception as e:
            # print(f"Failed to send command {command}: {e}")
            return None

    def send_skip_command(self, command):
        try:
            if not self.connected:
                print("Not connected to playback server. Trying to connect...")
                if not self.init_connection():
                    return None

            self.tn3.write(command.encode('ascii') + b"\n")

            # Read response from the server
            response = self._read_response_skip()
            print("Response from playback server:", response)

            return response
        except Exception as e:
            print(f"Failed to send command {command}: {e}")
            return None

    def _read_response(self):
     try:
        next_line = ""

        # Read lines until finding the line containing "202 OK"
        while True:
            line = self.tn2.read_until(b"\n", timeout=5).decode('ascii').strip()
            if "202 OK" in line:
                break

        # Read the next line which contains the main data
        data_line = self.tn2.read_until(b"\n", timeout=5).decode('ascii').strip()

        # Check if the data line has the desired format
        if data_line.startswith('0 playing'):
            next_line = data_line
        if(data_line.startswith('0 paused')):
            next_line= data_line
        return next_line

     except EOFError:
        print("End of file reached while reading USTA U0 response.")
        return None

    def _read_response_skip(self):
     try:
        next_line = ""

        # Read lines until finding the line containing "202 OK"
        while True:
            line = self.tn3.read_until(b"\n", timeout=5).decode('ascii').strip()
            if "202 OK" in line:
                break

        # Read the next line which contains the main data
        data_line = self.tn3.read_until(b"\n", timeout=5).decode('ascii').strip()

        # Check if the data line has the desired format
        if data_line.startswith('0 playing'):
            next_line = data_line
        if(data_line.startswith('0 paused')):
            next_line= data_line
        return next_line

     except EOFError:
        print("End of file reached while reading USTA U0 response.")
        return None

    def Load_unload_logo(self) :

       # Toggle the value of is_logo_overlayed
        self.is_logo_overlayed = not self.is_logo_overlayed

        if self.is_logo_overlayed:
         #self.api.Firelogo()             #through spx
         if(self.render_logo.overlay("on")) :  #through moksha
          return f"{self.global_context.get_value('channel_name')}^_^LOAD-UNLOAD-LOGO^_^ACK "
        else :
         #self.api.Stop_logo()            #through spx
         if(self.render_logo.overlay("off")) :  #through moksha
          return f"{self.global_context.get_value('channel_name')}^_^LOAD-UNLOAD-LOGO^_^ACK "

    # #works ok
    # def _read_response(self):
    #     response = ""
    #     while True:
    #             line = self.tn.read_until(b"202 OK", timeout=5).decode('ascii').strip()
    #             # if line.strip() == "200 OK":
    #             #  continue
    #             # # if line.strip().startswith("\n"):
    #             # #  continue

    #             # if not line:
    #             #     continue
    #             # response += line
    #             # Check for the '202 OK' line
    #             if "202 OK" in line:
    #                 break
    #         # Read the next line which contains the main data
    #     data_line = self.tn.read_until(b"\n", timeout=2).decode('ascii').strip()
    #     response += data_line.replace('\n','')
    #     return response

    def stop_melted(self):

        #instead of resetting melted, trying clearing melted unit
        self.reset_melted_after_stop()
        
        self.stop_update_thread()
        self.update_thread_stop_event.set()
        self.append_thread_stop_event.set()
        # self.shutdown_melted()
        appended_guids.clear()

        #On stop, Panic all graphics
        #self.api.Panic()

        #on stop, delete service at Ts switcher
        # self.switcher.stop_service() #asianet no need
        self.flush_remaining_asrun_before_shutdown()
        self.df_manager.clear_dataframe()
        self.global_context.set_value("running_id","")
        self.global_context.set_value("elapsed_frames","")
        self.global_context.set_value("elapsed_time","")
        self.global_context.set_value("total_frames","")
        self.global_context.set_value("remaining_time","")

        self.global_context.set_value('sync',1)

        self.global_context.set_value("previous_on_air_primary_index","")

        print("Playback server stopped successfully.")
        logger.info("Playback server stopped successfully.")
        return True

    def clean_melted(self):
            response = self.send_command("CLEAN U0")
            response = self.send_command("REMOVE U0 0")
            # status="automation_stop"
            # self.global_context.set_value("automsg",status)
            print("Playback server unit cleaned successfully.")
            return True
        #else:
            print("Failed to stop playback server.")
            return False
        
    def shutdown_melted(self):
            response = self.send_command("SHUTDOWN")
            # status="automation_stop"
            # self.global_context.set_value("automsg",status)
            return True
        #else:
            print("Failed to stop playback server.")
            return False

    def kill_melted(self):
     try:
        result = subprocess.run(["pkill", "melted"], check=True)
        print("Playback process terminated successfully.")
        self.clear_dataframe()
        sys.exit(0)

     except subprocess.CalledProcessError as e:
        print(f"Error occurred while killing playback server: {e}")
        print(f"Output: {e.output}")

    def clear_dataframe(self):
        # Clear the DataFrame
        df = self.df_manager.get_dataframe()
        df.drop(df.index, inplace=True)
        print("DataFrame cleared.")

    def flush_remaining_asrun_before_shutdown(self):
   
        try:
            formatter=AsRunFormatter()
            current_date = datetime.date.today().strftime("%Y-%m-%d")
            asrun_file_path = os.path.join(
                self.global_context.get_value('asrun_location'),
                f"{self.global_context.get_value('channel_name')}-{current_date}"
            )
            logger.info(f"Flushing unwritten asrun data to file: {asrun_file_path}")
            formatter.write_to_file(asrun_file_path)
            formatter.send_batch_to_api()
            logger.info("Asrun data flush complete.")
        except Exception as flush_ex:
            logger.error(f"Error flushing remaining asrun data before shutdown: {flush_ex}", exc_info=True)
            
    def get_event_time_for_on_air_guid(self):
        if self.global_df is not None and self.on_air_guid is not None:
            event_row = self.global_df.loc[self.global_df['GUID'] == self.on_air_guid]
            if not event_row.empty:
                event_time = event_row.iloc[0]['EventTime']
                return event_time
        return None

    def skip_to_next(self):
     try:
        if not self.connected:
            print("Not connected to playback server. Trying to connect...")
            if not self.init_connection():
                return None

        # First, send the USTA U0 command to get the current status
        usta_command = "USTA U0"
        usta_response = self.send_skip_command(usta_command)

        if usta_response:
            # Extract the line that starts with '0 playing'
            for line in usta_response.splitlines():
                if line.startswith('0 playing'):
                    # Extract the last value which is the runningindex
                    runningindex = int(line.split()[-1])
                    break
            else:
                print("Failed to find '0 playing' line in USTA response")
                return None
        else:
            print("Failed to get USTA response")
            return None

        # Now proceed with the skip command using the runningindex
        command = f"GOTO U0 {runningindex} +1"
        response = self.send_command(command)
        self.global_context.set_value("automsg", "playing")

        if response:
            print("Successfully skipped to next ID:", response)
            return response
        else:
            print("Failed to skip to next ID")
            return None
     except Exception as e:
        print(f"Failed to send skip command: {e}")
        return None

    def update(self, command):
        
        if self.append_thread and self.append_thread.is_alive():
                logger.warning("Please wait for server to process append playlist!.")
                return f"{self.global_context.get_value('channel_name')}^_^UPDATE^_^NACK^_^Please wait for server to process append playlist!"
         
        
        parts = command.split('^_^')
        if len(parts) < 5:
            return "Invalid command format"

        guid = parts[2]
        values = parts[3].split(',')
         # Ignore the first element in the values list as it is the index
        values = values[1:]


        df = self.df_manager.get_dataframe()
        df_primary = df[~df['Type'].str.lower().isin(['sec', 'secondary'])].reset_index(drop=True)



        if guid not in df['GUID'].values:
            return "GUID not found in the DataFrame"

        row_index = df[df['GUID'] == guid].index[0]
        columns = df.columns.tolist()


        #if clipid has been updated to the correct one
        if df.iloc[row_index]['Inventory']is not values[2] and values[10]=="N.A" :
            clip_ext = self.get_file_extension(values[2])
            if clip_ext is not None :
                logger.info(f"updated ID to {values[2]} exists!" )
                
                self.df_manager.update_row(row_index, 'Inventory', values[2])
                self.df_manager.update_row(row_index, 'Status', 'O.K')
                
                if not (self.global_context.get_value("verification_mode").startswith('Physical')):
                    self.db_verify.verify_content(values[2])
                else :
                    verifier = PlaylistVerifier()
                    verifier.full_playlist_verification()
                
                self.global_context.set_value('sync',1)
                
                path= os.path.join(self.global_context.get_value('ContentLoc'),f"{values[2]}{clip_ext}")
                
                df=self.df_manager.get_dataframe()
                som = self.parse_time_to_frames(df[df["GUID"] == guid]["SOM"].iloc[0],)
                eof = som + self.parse_time_to_frames(df[df["GUID"] == guid]["Duration"].iloc[0],)
                position = df_primary[df_primary["GUID"]==values[17]].index[0]
                
                logger.info(f"Inserting into server : {values[2]}") 

                # Construct the Melted command to insert into the Melted unit
                melted_command = f"INSERT U0 {values[2]}{clip_ext} {position} {som} {eof}"
                try:
                        self.send_command(melted_command)
                        logger.info(f"Playback command executed: {melted_command}")
                        print(f"Playback command executed: {melted_command}")
                except Exception as melted_ex:
                        logger.error(f"Error executing playback command: {melted_ex}")

                # Insert the new GUID into the appended_guids list at the specified index
                self.insert_guid(guid,position)
                
                
                
                for col, val in zip(df.columns, values):
                    if col not in ['SOM', 'Duration','Inventory','Status']:
                        self.df_manager.update_row(row_index, col, val)   
                
            else :
                logger.info("Updated ID doesn't exist,keeping status N.A")
                for i, value in enumerate(values):
                  if i < len(columns):
                   self.df_manager.update_row(row_index, columns[i], value) 
        
        self.global_context.set_value("sync",1)
        return f"{self.global_context.get_value('channel_name')}^_^UPDATE^_^{guid}^_^{parts[3]}"

    def send_current_running_id_over_udp(self):
            try:
                details = self.get_current_running_id_details()
                formatted_message = self.format_details_as_string(details)
                self.send_udp_message(formatted_message)
                print("Message sent over UDP:", formatted_message)
            except Exception as ex:
                print("Error sending message over UDP:", ex)

    def get_current_running_id_details(self):

      try:
        df = self.df_manager.get_dataframe()

        # Retrieve the onair_index from the global context
        onair_index = self.global_context.get_value("onair_index")

        if onair_index is not None and onair_index < len(appended_guids):
            # Retrieve the on_air_guid from the appended_guids list using the onair_index
            on_air_guid = appended_guids[onair_index]

            # Locate the row in the DataFrame using the on_air_guid
            current_running_row = df.loc[df['GUID'] == on_air_guid]

            if not current_running_row.empty:
                current_running_details = current_running_row.iloc[0].to_dict()
                return current_running_details
            else:
                print(f"GUID {on_air_guid} not found in the DataFrame")
                return None
        else:
            print(f"Invalid onair_index {onair_index}")
            return None
      except Exception as ex:
          print("[ERROR] Error while fetching current running ID details : {ex}")

    def format_details_as_string(self, details):
     return ','.join(str(value) for value in details.values())

    def send_udp_message(self, message):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(message.encode(), (self.udp_ip, self.udp_port))

    def write_next_200_rows_to_csv(self, current_running_guid, csv_file_path):
        try:

            df = self.df_manager.get_dataframe()

            # # filtered_df = df[df['Type'].str.lower().isin(['pri', 'primary'])]


            # Find the index of the current running GUID
            # current_index = filtered_df.index[filtered_df['GUID'] == current_running_guid].tolist()

            # if current_index:
            #     current_index = current_index[0]
            
             # Get current on-air primary index from global context
            onair_indice = self.global_context.get_value('onair_indice')

            if onair_indice != "":
                onair_indice = int(onair_indice)
                    
                # Slice the DataFrame from the given index onward
                df_new = df.iloc[onair_indice + 1 :]
                
                filtered_df = df_new[
                        df_new['Type'].str.lower().isin(['pri', 'primary']) &
                        ~df_new['Inventory'].str.startswith('LIVE', na=False)
                    ]
                
                 # Take the first 500 rows
                next_rows = filtered_df.head(500)

                # # Get the next 200 rows from the current index
                # next_200_rows = filtered_df.iloc[onair_indice +1 : onair_indice + 501]


                # Filter rows to include only those with Type as PRI, pri, primary, or PRIMARY
                # filtered_rows = next_200_rows[
                # next_200_rows['Type'].str.lower().isin(['pri', 'primary'])&
                # ~next_200_rows['Inventory'].str.startswith("LIVE", na=False)
                # ]


                # Write the filtered rows to a CSV file
                next_rows.to_csv(csv_file_path, index=False)
                print(f"Written next 200 filtered rows to {csv_file_path}")
                 # Trigger busfile only once
                if not self.is_busfile_triggered:
                        self.busfile_trigger()
                        self.is_busfile_triggered = True  # Set flag to True



            else:
                print(f"GUID {current_running_guid} not found in the DataFrame")

        except Exception as ex:
                print("Busfile creation error")
                logger.error(f"Busfile creation error :{ex}")

    def busfile_trigger(self):

        url = "http://127.0.0.1:8050/busfile_monitor"

        data = {
            "channel_id": self.global_context.get_value('channel_id')
        }

        headers = {'Content-Type': 'application/json'}

        try:

            response = requests.post(url, data=json.dumps(data), headers=headers)
            # response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
                print(f"HTTP Error: {e.response.status_code}")
        except Exception as e:
            print(f"An error occurred: {str(e)}")

    def get_file_extension(self, filename):
      for root, _, files in os.walk(self.global_context.get_value("ContentLoc")):
            for file in files:
                if file.startswith(filename) and file[len(filename):len(filename)+1] == ".":
                    return os.path.splitext(file)[1]
      return None

    def collect_secondary_guids(self, df, previous_on_air_guid):
        all_guids = df["GUID"].tolist()

        # Initialize list to hold secondary GUIDs
        sec_guids = []
        start_collecting = False

        for guid in all_guids:
            if guid == previous_on_air_guid:
                start_collecting = True
                continue  # Skip the current on-air GUID

            if start_collecting:
                event_row = df[df['GUID'] == guid]
                if event_row.empty:
                    continue

                event_type = event_row.iloc[0]['Type'].lower()

                if event_type in ["pri", "primary"]:
                    break  # Stop if the next PRI type ID is encountered

                if event_type in ["sec", "secondary"]:
                    sec_guids.append(guid)

        return sec_guids

    def Jump_to_live(self):

      try:
        self.stop_update_thread()
        self.update_thread_stop_event.set()
        self.append_thread_stop_event.set()

        if(self.global_context.get_value("IsLive")):

                live=live_service()
                logger.info(f"Live module is enabled, sending Live packet now!")

                current_guid=self.global_context.get_value('onair_guid')
                df = self.df_manager.get_dataframe()
                try:

                    current_index = df[df['GUID'] == current_guid].index[0]

                    for idx in range(current_index + 1, len(df)):
                        inventory =df.loc[idx, 'Inventory']
                        if inventory.upper().startswith('LIVE'):

                            nearest_live_guid=df.loc[idx, 'GUID']
                            self.global_context.set_value("onair_guid",nearest_live_guid)
                            self.global_context.set_value('Current_live_guid',nearest_live_guid)

                            current_index = df.index[df['GUID'] == nearest_live_guid].tolist()[0]
                            #after_live_id= df.iloc[current_index + 1]['GUID']

                            after_live_id = df.iloc[current_index + 1:][df['Type'].str.lower().isin(["pri", "primary"])].iloc[0]['GUID']

                            self.global_context.set_value('ready_guid', after_live_id)


                            #ts switcher api to server
                            self.switcher.switch_service(1)
                            scte_service.restart_service() #restarting scte service

                            reset_thread = threading.Thread(target=self.reset_melted,args=(after_live_id,))
                            reset_thread.start()

                            event_row = df.loc[df['GUID'] == self.on_air_guid]
                            if not event_row.empty:
                             event_time = event_row.iloc[0]['EventTime']


                            status = "playing"
                            self.global_context.set_value("automsg",status)
                            self.global_context.set_value("status", status)
                            self.global_context.set_value("running_id", inventory)
                            self.global_context.set_value("elapsed_frames", "0")
                            self.global_context.set_value("total_frames", "0")
                            self.global_context.set_value("elapsed_time", "0")
                            self.global_context.set_value("remaining_time", "00:00:00:00")
                            # self.global_context.set_value("channel_props", self.global_context.get_value("Playlist_loaded"))
                            self.global_context.set_value('onair_guid',nearest_live_guid)
                            self.global_context.set_value('ready_guid', after_live_id)
                            self.global_context.set_value("event_time",timemgt.get_system_time_ltc())
                            # self.global_context.set_value("InProgress",1)

                          #Hide-Unhide live on the basis of set configuration
                            if(self.global_context.get_value("logo_hide_on") is not False and self.global_context.get_value("logo_hide_on") == "live" ) :
                                    # self.api.Stop_logo()              #through spx
                                    if(self.render_logo.overlay("off")) :   #through moksha
                                     print("Stopped logo rendering, as Logo hide on Live set to true!")
                                     self.is_logo_overlayed=False

                            #not in use rn
                            # self.live_thread = threading.Thread(target=live.process_live_udp,args=(inventory,))
                            # self.live_thread.start()

                            # self.reset_melted(nearest_live_guid,inventory)
                            break

                    # print("No 'Live' inventory found after the current GUID.")
                    print(f"Command sent : {self.global_context.get_value('channel_name')}^_^LIVE-SWITCH^_^ACK ")
                    return f"{self.global_context.get_value('channel_name')}^_^LIVE-SWITCH^_^ACK"

                except IndexError:
                    print("The current GUID is not found in the DataFrame.")
                    return None
        else:
                logger.info(f"Live module disabled for channel : {self.global_context.get_value('channel_name')}")

      except Exception as ex:
                    print(f"[EXCEPTION] An unexpected error occurred while switching to Live: {ex}")
                    self.logger.error(f"An unexpected error occurred while switching to Live: {ex}")

    def Jump_to_live_linear(self,Inventory):

        try:

            self.update_thread_stop_event.set()
            self.append_thread_stop_event.set()
            # self.stop_update_thread()

            if(self.global_context.get_value("IsLive")):

                                live=live_service()
                                logger.info(f"Live module is enabled, sending Live packet now!")

                                current_guid=self.global_context.get_value('onair_guid')
                                df = self.df_manager.get_dataframe()


                                current_index = df[df['GUID'] == current_guid].index[0]

                                after_live_id = df.iloc[current_index + 1:][df['Type'].str.lower().isin(["pri", "primary"])].iloc[0]['GUID']

                                self.global_context.set_value('ready_guid', after_live_id)


                                #ts switcher api to server
                                self.switcher.switch_service(1)
                                scte_service.restart_service() #restarting scte service

                                reset_thread = threading.Thread(target=self.reset_melted,args=(after_live_id,))
                                reset_thread.start()

                                event_row = df.loc[df['GUID'] == self.on_air_guid]


                                status = "playing"
                                self.global_context.set_value("automsg",status)
                                self.global_context.set_value("status", status)
                                self.global_context.set_value("running_id", Inventory)
                                self.global_context.set_value("elapsed_frames", "0")
                                self.global_context.set_value("total_frames", "0")
                                self.global_context.set_value("elapsed_time", "0")
                                self.global_context.set_value("remaining_time", "00:00:00:00")
                                # self.global_context.set_value("channel_props", self.global_context.get_value("Playlist_loaded"))
                                self.global_context.set_value('onair_guid',current_guid)
                                self.global_context.set_value('ready_guid', after_live_id)
                                self.global_context.set_value("event_time",timemgt.get_system_time_ltc())
                                # self.global_context.set_value("InProgress",1)

                            #Hide-Unhide live on the basis of set configuration
                                if(self.global_context.get_value("logo_hide_on") is not False and self.global_context.get_value("logo_hide_on") == "live" ) :
                                         # self.api.Stop_logo()              #through spx
                                        if(self.render_logo.overlay("off")) :   #through moksha
                                         print("Stopped logo rendering, as Logo hide on Live set to true!")
                                         self.is_logo_overlayed=False

                                #not in use rn
                                # self.live_thread = threading.Thread(target=live.process_live_udp,args=(inventory,))
                                # self.live_thread.start()

                                # self.reset_melted(nearest_live_guid,inventory)


                        # print("No 'Live' inventory found after the current GUID.")
                                print(f"Command sent : {self.global_context.get_value('channel_name')}^_^LIVE-SWITCH^_^ACK ")
                                return f"{self.global_context.get_value('channel_name')}^_^LIVE-SWITCH^_^ACK"


            else:
                            logger.info(f"Live module disabled for channel : {self.global_context.get_value('channel_name')}")

        except Exception as ex:
                        print(f"[EXCEPTION] An unexpected error occurred while switching to Live: {ex}")
                        self.logger.error(f"An unexpected error occurred while switching to Live: {ex}")

    def reset_melted(self,after_live_id):


            self.kill_melted_forcefully()
            IsStarted=self.start_melted_again()
            print("Playback server restarted!")

            appended_guids.clear()
            if(IsStarted):
                if self.init_connection():
                    self.send_inventory_commands_afterlive(after_live_id)

    def reset_melted_after_stop(self):


            self.kill_melted_forcefully()
            IsStarted=self.start_melted_again()

            appended_guids.clear()
            if(IsStarted):
                if self.init_connection():
                  print('Ready to load new playlist!')

    def kill_melted_forcefully(self):
        try:
            self.stop_update_thread()
            self.update_thread_stop_event.set()
            self.append_thread_stop_event.set()


                # Attempt to stop threads within a timeout
            if self.update_thread and self.update_thread.is_alive():
                self.update_thread.join(timeout=5)  # Wait for 5 seconds
                if self.update_thread.is_alive():
                    print("Force-stopping update thread...")
                    self.force_stop_thread(self.update_thread)

            if self.append_thread and self.append_thread.is_alive():
                self.append_thread.join(timeout=5)  # Wait for 5 seconds
                if self.append_thread.is_alive():
                    print("Force-stopping append thread...")
                    self.force_stop_thread(self.append_thread)


            port = self.server_port
            # is_killed = self.kill_melted_by_port(port)
            is_killed = self.shutdown_melted()
            if is_killed:
                logger.info(f"Successfully killed melted on port {port}.")
            else:
                logger.info(f"No melted process found on port {port}.")

        except subprocess.CalledProcessError as e:
         print(f"Error while terminating playback server: {e}")


    def kill_melted_by_port(self, port):
            try:
                result = subprocess.run(
                    ["lsof", "-i", f":{port}"],
                    capture_output=True,
                    text=True,
                    check=True
                )
                lines = result.stdout.strip().split("\n")
                if len(lines) > 1:
                    pid = lines[1].split()[1]
                    
                
                kill_result = subprocess.Popen(["kill", "-9", str(pid)])

                if kill_result.returncode == 0:
                    logger.info(f"Successfully killed melted on port {port} (PID {pid})")
                else:
                    logger.warning(f"kill command returned non-zero exit code: {kill_result.returncode}")

                # Step 3: Confirm it's dead
                time.sleep(1)
                confirm = subprocess.run(
                    ["lsof", "-i", f":{port}"],
                    capture_output=True,
                    text=True
                )
                if confirm.stdout.strip():
                    logger.warning(f"Process still using port {port} after kill attempt.")
                    return False
                else:
                    logger.info(f"Confirmed melted process on port {port} has been terminated.")
                    return True
                            
            except subprocess.CalledProcessError as e:
                logger.warning(f"Failed to find/kill melted on port {port}: {e}")
                return False

    def kill_melted_tmux(self):
        try:
            session_name = f"melted_{self.server_port}"
            subprocess.run(["tmux", "kill-session", "-t", session_name], check=True)
            logger.info(f"Killed tmux session: {session_name}")
            return True
        except subprocess.CalledProcessError:
            logger.warning(f"No tmux session found: {session_name}")
            return False

    def request_external_melted_kill(self, port):
        try:
            response = requests.post(f"http://localhost:{self.global_context.get_value('listener_port') + 1000}/api/kill_by_port", json={"port": port})
            logger.info(f"Kill response from APIListener: {response.text}")
            if response.status_code == 200 :
                return True
            
        except Exception as e:
            logger.error(f"Failed to call kill_by_port: {e}")
            return False


    def start_melted_again(self, max_retries=3, wait_time=2):
        try:

            mode = self.global_context.get_value("OutMode")
            print(f"mode selected : {mode}")
            logger.info(f'mode selected : {mode}')
            config_path = "udp_hdp.conf" if mode == 0 else "ndi.conf" if mode == 1 else "decklink.conf" if mode == 2 else None


            for attempt in range(max_retries):
                print(f"Attempting to start playback server (Attempt {attempt + 1}/{max_retries})...")

                # Start the melted executable with the appropriate config
                command = ["./melted", "-c", config_path, "-port", str(self.server_port)]
                subprocess.run(command, check=True, cwd=self.melted_executable_path)
            
                # Wait a few seconds to allow melted to initialize
                time.sleep(wait_time)

                if self.check_melted_running_on_port(self.server_port):
                    logger.info("Playback server started successfully.")
                    return True
                else:
                    logger.warning(f"Melted not detected on port {self.server_port}. Retrying...")

            logger.error("Failed to start playback server after multiple attempts.")
            return False

        except subprocess.CalledProcessError as ex:
            logger.error(f"Error starting playback server: {ex}")
            return False



    def check_melted_running_on_port(self, port):
        try:
            result = subprocess.run(["lsof", "-i", f":{port}"], capture_output=True, text=True)
            return bool(result.stdout.strip())
        except Exception as e:
            logger.error(f"Error checking if melted is running on port {port}: {e}")
            return False


    def switch_to_server(self):

            try:
                    self.global_context.set_value("IsLiveRunning",False)
                    df = self.df_manager.get_dataframe()
                    guids = df["GUID"]

                    play_response = self.send_command("PLAY U0")

                    # threading.Thread(target=self.api.Firelogo, args=()).start()       #through spx
                  # threading.Thread(target=self.render_logo.overlay, args=("on",)).start()  #through moksha

                    if(self.render_logo.overlay("on")) :   #through moksha
                        print("Started logo rendering")
                        self.is_logo_overlayed=True

                    self.on_air_guid=self.global_context.get_value('ready_guid')
                    try:
                                service=server_switch_service()
                                self.global_context.set_value('Current_live_guid', "")
                                logger.info(f"Live module is enabled, sending server switch packet now!")
                                #threading.Thread(target=service.execute).start()
                                self.switcher.switch_service(0)
                                scte_service.restart_service() #restarting scte service
                    except Exception as ex:
                                    print(ex)


                    self.global_context.set_value("onair_guid",self.on_air_guid)

                    self.df_manager.toggle_up_dataframe()
                    self.start_append_thread(df, guids,self.on_air_guid)
                    self.start_update_thread()

                    return f"{self.global_context.get_value('channel_name')}^_^PLAY^_^ACK"
            except Exception as ex:
                    print(f"[EXCEPTION] An unexpected error occurred while switching to server: {ex}")
                    self.logger.error(f"An unexpected error occurred while switching to server: {ex}")

    def append_playlist(self,Playlist, PlaylistDate):
        try:
            
            if self.append_thread and self.append_thread.is_alive():
                logger.warning("Load playlist already running. Cannot start another playlist append.")
                return f"{self.global_context.get_value('channel_name')}^_^APPEND-PLAYLIST^_^NACK^_^Load playlist already running, wait for sometime!   "
            
            
            df = self.df_manager.get_dataframe()
            loc = self.global_context.get_value("PlaylistLoc")

        
            # Load the new DataFrame from the CSV file
            new_file_path = os.path.join(loc, Playlist)
            new_df = pd.read_csv(new_file_path, header=None, skipinitialspace=True, na_filter=False, skip_blank_lines=True)
            #adding playlist name & playlist actual index to every row
            new_df['playlist'] = Playlist
            # new_df['playlist_index'] = range(len(new_df))
            
            print(f"Now appending playlist : {new_file_path}")
            logger.info(f"Playlist location selected : {new_file_path}")

            # Access the existing DataFrame
            # df_manager = DataFrameManager()
            # existing_df = df_manager.get_dataframe()

            if df is None:
                self.df_manager._df = new_df
            else:
                # Ensure column names are consistent
                new_df.columns =df.columns
                # Generate a GUID for each new row
                new_df['GUID'] = new_df['GUID'].apply(lambda x: self.df_manager.generate_guid())
                new_guids =  new_df['GUID']
                

                
                combined_df = pd.concat([df, new_df], ignore_index=True)

                # Update the DataFrame in the DataFrameManager instance
                self.df_manager._df = combined_df
                
                
                
                if not (self.global_context.get_value("verification_mode").startswith('Physical')):
                    logger.info("performing database verification before appending new playlist")
                    self.db_verify.verify()
                else :
                    logger.info("performing physical verification before appending new playlist")
                    verifier = PlaylistVerifier()
                    verifier.full_playlist_verification()
                    
                self.global_context.set_value('sync',1)   
                df = self.df_manager.get_dataframe()


                rows_count = len(self.df_manager._df)
                # logger.info(f"Rows count in new appended playlist : {rows_count}")
                self.global_context.set_value("rows_count", rows_count)
                first_row_guid = new_df.iloc[0]["GUID"] if rows_count > 0 else ""   #why are you u[dating first row here?
                last_row_guid = self.df_manager._df.iloc[-1]["GUID"] if rows_count > 0 else ""
                self.global_context.set_value("last_row_guid", last_row_guid)
                
                self.global_context.set_value("last_guid",last_row_guid)

                self.df_manager.toggle_up_dataframe()
                self.global_context.set_value('sync',1)
                
                print(f"Total rows to be appended :{len(new_guids)}")
                # logger.info(f"NEW PLAYLIST APPENDED : {Playlist} with rows :{len(new_guids)}")

                
                self.start_append_thread(self.df_manager._df, new_guids,first_row_guid)
                # self.send_inventory_commands_after_append(new_guids,first_row_guid)
                logger.info(f"NEW PLAYLIST APPENDED : {Playlist} with rows :{len(new_guids)}")
                

            # Return the combined DataFrame as lines
            return self.df_manager.get_lines()



        except FileNotFoundError:
            print("File not found.")
            return []

    def parse_time_to_frames(self,time_str):
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

    def force_stop_thread(self, thread):
     if thread.is_alive():
        # Warning: This method assumes the thread checks `self.<event>.is_set()` regularly.
        print(f"Force-stopping thread {thread.name}.")
        # You may need to clean up any lingering state or variables if the thread does not terminate.

    def add_asrun_list(self,played_guid):
         # Check and add the already playeed guid to the asrun list
            if int(self.global_context.get_value("asrun_filter")) in [0,1] :
                if played_guid not in self.global_context.get_value('asrun_list'):
                    self.global_context.get_value('asrun_list').append(played_guid)
                    print(f"Added {played_guid} to Asrun list")
                else:
                    print(f"GUID {played_guid} is already in the Asrun list.")

        # to add sec attached to that primary id that has been played
            if int(self.global_context.get_value("asrun_filter")) in [0,2] :
                df = self.df_manager.get_dataframe()
                sec_list=self.collect_secondary_guids(df,played_guid)

                # Add all secondary GUIDs to the asrun list if not already present
                new_guids = [guid for guid in sec_list if guid not in asrun_list]
                asrun_list.extend(new_guids)


                if new_guids:
                    print(f"Added secondary GUIDs {new_guids} to Asrun list")
                else:
                    print("No new secondary GUIDs to add to Asrun list")

    def shortened_df(self, running_guid):

            print(f"Updating DataFrame for running GUID: {running_guid}")

            df = self.df_manager.get_dataframe()

            if not df.empty:
                # Drop only the top row of the DataFrame
                df = df.iloc[1:]  # Keep all rows except the first one

                # Update the DataFrame in the manager
                self.df_manager._df = df
                
                melted_command = "REMOVE U0 0"
                try:
                        self.send_command(melted_command)
                        logger.info(f"Playback command executed: {melted_command}")
                except Exception as ex:
                        logger.error(f"Error executing playback command: {ex}")

                appended_guids.pop(0)
            
                self.df_manager.toggle_up_dataframe()
                self.global_context.set_value('sync', 1)
                logger.info("DataFrame updated. Removed the top row.")
            else:
                logger.warning("DataFrame is empty. No rows to remove.")

            # df = self.df_manager.get_dataframe()

            # row_index = df[df['GUID'] == running_guid].index

            # if not row_index.empty:
            #         target_index = row_index[0]

            #     # if target_index >= 10:
            #         start_index = target_index - 10
            #         df = df.iloc[start_index:1]

            #         #Update the DataFrame in the manager
            #         self.df_manager._df = df
            #         self.df_manager.toggle_up_dataframe()
            #         self.global_context.set_value('sync',1)
            #         logger.info(f"DataFrame updated. Kept rows from index {start_index} to {target_index}.")
            #     # else:
            #     #     logger.info("Less than or equal to 10 GUIDs have been played. No rows removed from the DataFrame.")
            # else:
            #     logger.warning(f"No row found with GUID: {running_guid}. No changes made to the DataFrame.")

    def handle_logo_on_break(self):

            if self.is_break and self.is_logo_overlayed:
                if (self.global_context.get_value("logo_hide_on") is not False and
                    self.global_context.get_value("logo_hide_on") == "break"):
                    if self.render_logo.overlay("off"):
                        print("Stopped logo rendering, as Logo hide on break set to true!")
                        logger.info("Stopped logo rendering, as Logo hide on break set to true!")
                        self.is_logo_overlayed = False


            return self.is_break

   
