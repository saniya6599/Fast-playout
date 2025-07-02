import datetime
import socket
import os
import subprocess
import telnetlib
import threading

import pandas as pd
from CommonServices.Global_context import GlobalContext
from CommonServices.DataFrameManager import DataFrameManager
from CommonServices.Time_management import time
from CommonServices.config import Config


class MeltedClient:
    def __init__(self):
        self.server_ip = ".27.46"
        self.server_port = 5250
        self.melted_executable_path = "/home/pmsl/melted/src/melted/src/melted"
        self.connected = False
        self.client_socket = None
        self.update_thread = None
        self.update_thread_stop_event = threading.Event()
        self.tn = None
        self.global_context = GlobalContext()
        self.df_manager = DataFrameManager()
        self.df_manager.get_dataframe()

    def init_connection(self):
        try:
            self.tn = telnetlib.Telnet(self.server_ip, self.server_port)
            self.connected = True
            return True
        except Exception as e:
            print(f"Failed to connect to Melted server: {e}")
            return False

    def send_command(self, command):
     try:
        if not self.connected:
            print("Not connected to Melted server. Trying to connect...")
            if not self.init_connection():
                return None

        self.tn.write(command.encode('ascii') + b"\n")
        response = self.tn.read_until(b"\n\n", timeout=2).decode('ascii')
        print("Response from Melted server:", response)
        return response
     except Exception as e:
        print(f"Failed to send command {command}: {e}")
        return None

    def parse_usta(self, response):
       lines = response.split('\n')
       if len(lines) > 1:
            data = lines[1].split(' ')
            status = data[1]
            filename = data[2].strip('"')
            elapsed_frames = int(data[3])
            total_frames = int(data[4])
            return status, filename, elapsed_frames, total_frames
       return None, None, None, None
    
   
    def start_update_thread(self):
        if self.update_thread and self.update_thread.is_alive():
            self.update_thread_stop_event.set()
            self.update_thread.join()

        self.update_thread_stop_event.clear()
        self.update_thread = threading.Thread(target=self.update_status_periodically)
        self.update_thread.start()

    def update_status_periodically(self):
        while not self.update_thread_stop_event.is_set():
            response = self.send_command("USTA U0")
            if response:
                status, filename, elapsed_frames, total_frames = self.parse_usta(response)
                if status  == "Playing":
                    onair_guid = self.get_onair_guid()
                    ready_guid = self.get_next_ready_guid()
                    self.global_context.set_value("onair_guid",onair_guid)
                    self.global_context.set_value("ready_guid", ready_guid)

                    self.global_context.set_value("status", status)
                    self.global_context.set_value("filename", filename)
                    self.global_context.set_value("elapsed_frames", elapsed_frames)
                    self.global_context.set_value("total_frames", total_frames)
                    self.global_context.set_value("elapsed_time", self.calculate_elapsed_time(elapsed_frames))
                    self.global_context.set_value("remaining_time", self.calculate_remaining_time(total_frames, elapsed_frames))
                    self.global_context.set_value("channel_props", os.path.basename(filename))

            time.sleep(1)

    def start_melted_server(self):
        try:
            subprocess.run(["./melted"], check=True, cwd=self.melted_executable_path)
            print("Melted server started.")
            return True
        except Exception as ex:
            print("Error starting Melted server:", ex)
            return False

    def send_inventory_commands(self, df_manager, GUID):
        try:
            df = df_manager.get_dataframe()
            guids = df["GUID"]
            rows_count=len(df)
            self.global_context.set_value("rows_count",rows_count)
            first_row_guid = df.iloc[0]["GUID"] if rows_count > 0 else ""
            last_row_guid = df.iloc[-1]["GUID"] if rows_count > 0 else ""
            self.global_context.set_value("first_row_guid",first_row_guid)
            self.global_context.set_value("last_row_guid",last_row_guid)
            start_appending = False

            for guid in guids:
                if guid == GUID:
                    start_appending = True

                if start_appending:
                    inventory_item = df[df["GUID"] == guid]["Inventory"].iloc[0]
                    command = f"APND U0 {inventory_item}.mp4"
                    response = self.send_command(command)

                    if guid == GUID:
                        play_response = self.send_command("PLAY U0")
                        self.start_update_thread()
                        if "OK" not in play_response:
                            print("PLAY command failed with response:", play_response)
                            return False

            return True
        except Exception as ex:
            print("Error sending inventory commands:", ex)
            return False

    def start(self, GUID):
        try:
            if self.start_melted_server():
                if self.init_connection():
                    clip_to_play = self.df_manager.get_clipid_by_guid(GUID)
                    if self.send_initial_commands():
                        # self.start_update_thread()
                        response = self.send_inventory_commands(self.df_manager, GUID)
                        return response
        except Exception as ex:
            print("Error:", ex)

    def stop_update_thread(self):
        if self.update_thread and self.update_thread.is_alive():
            self.update_thread_stop_event.set()
            self.update_thread.join()

    def __del__(self):
        self.stop_update_thread()
        if self.client_socket:
            self.client_socket.close()
        if self.tn:
            self.tn.close()

    def send_initial_commands(self):
        commands = ["UADD sdl", "SET root=/home/pmsl/videos"]
        for command in commands:
            response = self.send_command(command)
            if "OK" not in response:
                print(f"Command {command} failed with response: {response}")
                #return False
        return True

    def delete_entries_by_guids(self, guids_str):
     try:
        guids = guids_str.split('~')
        df = self.df_manager.get_dataframe()
        not_found_guids = []

        for guid in guids:
            if guid in df["GUID"].values:
                df = df[df["GUID"] != guid]
                print(f"Entry with GUID {guid} deleted.")
            else:
                not_found_guids.append(guid)
                print(f"GUID {guid} not found.")

        self.df_manager._df = df

        if not_found_guids:
            return f"MLT^_^DELETE^_^NACK^_^GUIDS_NOT_FOUND: {'~'.join(not_found_guids)}"
        else:
            return f"MLT^_^DELETE^_^{guids_str}^_^ACK"
     except Exception as ex:
        print("Error deleting entries:", ex)
        return "MLT^_^DELETE^_^NACK^_^ERROR"

    def insert_entry(self, command):
        try:
            parts = command.split("^_^")
            GUID = parts[2]
            data = parts[3].split(",")

            position = int(data[0]) - 1
            row_data = data[1:]

            new_guid = DataFrameManager.generate_guid()
            row_data[17] = new_guid

            df = self.df_manager.get_dataframe()

            new_row = pd.DataFrame([row_data], columns=df.columns)

            df1 = pd.concat(
                [df.iloc[:position], new_row, df.iloc[position:]]
            ).reset_index(drop=True)

            self.df_manager._df = df1

            print(f"Entry inserted at position {position} with GUID {new_guid}.")
            return f"MLT^_^INSERT^_^{new_guid}^_^OK"
        except Exception as ex:
            print("Error inserting entry:", ex)
            return "MLT^_^INSERT^_^NACK^_^ERROR"

    def get_onair_guid(self):
        response = self.send_command(f"USTA U0")
        status, filename, elapsed_frames, total_frames = self.parse_usta(response)
        
        if status == "Playing":
            df = self.df_manager.get_dataframe()
            onair_guid = df[df["Inventory"] == filename]["GUID"].iloc[0]
            self.global_context.set_value("onair_guid", onair_guid)
            return onair_guid
        return ""

    def get_next_ready_guid(self):
        response = self.send_command("LIST U0")
        lines = response.split("\n")
        df = self.df_manager.get_dataframe()
        
        if len(lines) > 2:
            next_filename = lines[2].split(" ")[1]
            ready_guid = df[df["Inventory"] == next_filename]["GUID"].iloc[0]
            self.global_context.set_value("ready_guid", ready_guid)
            return ready_guid
        return ""

    def calculate_elapsed_time(self, elapsed_frames, frame_rate=25):
        elapsed_seconds = elapsed_frames / frame_rate
        elapsed_td = datetime.timedelta(seconds=elapsed_seconds)
        return str(elapsed_td)

    def calculate_remaining_time(self, total_frames, elapsed_frames, frame_rate=25):
        remaining_frames = total_frames - elapsed_frames
        remaining_seconds = remaining_frames / frame_rate
        remaining_td = datetime.timedelta(seconds=remaining_seconds)
        return str(remaining_td)

    def paste_entries(self, command):
     try:
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
            return f"MLT^_^PASTE^_^{source_guids_str}^_^{target_guid}^_^NACK^_^ERROR: Target row not found"

        target_index = target_row.index[0]

        new_rows = []
        new_guids = []
        for source_guid in source_guids:
            # Find the row to copy using source_guid
            source_row = df.loc[df["GUID"] == source_guid]

            if not source_row.empty:
                source_row_data = source_row.iloc[0].astype(str).tolist()

                # Generate a new GUID for the new row
                new_guid = DataFrameManager.generate_guid()
                source_row_data[17] = new_guid  # Assuming the GUID column is at index 17

                # Create the new row DataFrame
                new_row_df = pd.DataFrame([source_row_data], columns=df.columns)
                new_rows.append(new_row_df)
                new_guids.append(new_guid)

                print(f"Row copied from GUID {source_guid} to new GUID {new_guid} above GUID {target_guid}.")
            else:
                print(f"Source row with GUID {source_guid} not found.")

        if new_rows:
            # Insert all new rows just above the target row
            df = pd.concat(
                [df.iloc[:target_index], *new_rows, df.iloc[target_index:]]
            ).reset_index(drop=True)

            # Update the DataFrame in the DataFrameManager instance
            df_manager._df = df

            return f"MLT^_^PASTE^_^{source_guids_str}^_^{target_guid}^_^ACK^_^{','.join(new_guids)}"
        else:
            return f"MLT^_^PASTE^_^{source_guids_str}^_^{target_guid}^_^NACK^_^ERROR: Source rows not found"
     except Exception as ex:
        print("Error pasting entries:", ex)
        return f"MLT^_^PASTE^_^{source_guids_str}^_^{target_guid}^_^NACK^_^ERROR"

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
                return f"MLT^_^VERIFY^_^{guid}^_^{row_str}"
            else:
                return f"MLT^_^VERIFY^_^{guid}^_^NACK^_^ERROR: Row not found"
        except Exception as ex:
            print("Error verifying entry:", ex)
            return f"MLT^_^VERIFY^_^{guid}^_^NACK^_^ERROR"

    # def send_usta_command(self, command):
    #     try:
    #         if not self.connected:
    #             print("Not connected to Melted server. Trying to connect...")
    #             if not self.init_connection():
    #                 return None

    #         self.tn.write(command.encode('ascii') + b"\n")
    #         response = ""
    #         while True:
    #             chunk = self.tn.read_until(b"\r\n").decode('ascii')
    #             response += chunk
    #             if "202 OK" in chunk:
    #                 break

    #         print("Response from Melted server:", response)
    #         return response
    #     except Exception as e:
    #         print(f"Failed to send command {command}: {e}")
    #         return None




































    # def start(self,GUID):
    #     try:
    #         global_context = GlobalContext()
    #         if self.start_melted_server():
    #             # Send commands to Melted server
    #             self.send_commands()
    #             return "MLT^_^DIRECT-PLAY^_^ACK"
    #     except Exception as ex:
    #         print("Error:", ex)
    #     finally:
    #         if self.connected:
    #             self.client_socket.close()
    #             print("Connection closed.")
