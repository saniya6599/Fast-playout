import socket
import os
import subprocess

import pandas as pd
from CommonServices.Global_context import GlobalContext
from CommonServices.DataFrameManager import DataFrameManager
from CommonServices.Time_management import time
from CommonServices.config import Config


class MeltedClient:
    def __init__(self):
        self.server_ip = "192.168.15.248"
        self.server_port = 5250
        self.melted_executable_path = "/home/pmsl/melted/src/melted/src/melted"
        self.melted_process_name = "melted"
        self.connected = False
        self.client_socket = None

    def connect_to_melted(self):
        try:
            # Connect to the Melted server
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.connect((self.server_ip, self.server_port))
            self.connected = True
            print("Connected to playback server.")
            return True
        except Exception as ex:
            print("Error connecting to playback server:", ex)
            return False

    def send_command(self, command):
        try:
            if not self.connected:
                print("Not connected to playback server. Trying to connect...")
                if not self.connect_to_melted():
                    return None

            # Send command to Melted server
            self.client_socket.sendall((command + "\n").encode())

            # Receive response from Melted server
            response = self.client_socket.recv(1024).decode()
            print("Response from playback server:", response)
            return response
        except Exception as ex:
            print("Error sending command to playback server:", ex)
            return None

    def start_melted_server(self):
        try:
            # Start the Melted server using relative path
            subprocess.run(["./melted"], check=True, cwd=self.melted_executable_path)
            print("Playback server started.")
            return True
        except Exception as ex:
            print("Error starting Melted server:", ex)
            return False

    # def send_commands(self):
    #         global_context = GlobalContext()
    #         df_manager = DataFrameManager()
    #         # Start Melted server
    #         config = Config()
    #         # df_manager.load_dataframe(config.get("file_path"))
    #         df_manager.get_dataframe()
    #         clip_to_play = df_manager.get_clipid_by_guid(global_context.get_value("GUID"))
    #         commands = ["UADD sdl", f"SET root=/home/pmsl/videos", f"APND U0 {clip_to_play}.mp4", "PLAY U0"]
    #         for command in commands:
    #          self.send_command(command)

    def send_initial_commands(self):
        commands = ["UADD sdl", "SET root=/home/pmsl/videos"]
        for command in commands:
            response = self.send_command(command)
            # if "OK" not in response:
            #     print(f"Command {command} failed with response: {response}")
            #     return False
        return True
    
    # def send_inventory_commands(self, df_manager, clip_to_play_guid):
    #     try:
    #         df = df_manager.get_dataframe()
    #         guids = df["GUID"]  # Assuming 'GUID' is the name of the column containing GUIDs

    #         # Flag to indicate when to start appending inventory items
    #         start_appending = False

    #         # Variables to store running and next queued inventory details
    #         running_inventory_details = None
    #         next_queued_guid = None
    #         next_next_queued_guid = None

    #         for i, guid in enumerate(guids):
    #             # If the current GUID matches the specified GUID, start appending items
    #             if guid == clip_to_play_guid:
    #                 start_appending = True

    #             if start_appending:
    #                 # Assuming inventory items are filenames, you may need to adjust this part
    #                 inventory_item = df[df["GUID"] == guid]["Inventory"].iloc[0]
    #                 command = f"APND U0 {inventory_item}.mp4"
    #                 response = self.send_command(command)

    #                 if "OK" not in response:
    #                     print(f"Command {command} failed with response: {response}")
    #                     return False

    #                 # Store the running inventory details for the response message
    #                 if running_inventory_details is None:
    #                     running_inventory_row = df[df["GUID"] == clip_to_play_guid].iloc[0]
    #                     running_inventory_details = "~".join(running_inventory_row.astype(str))

    #                     # Send the PLAY command after the first inventory item is appended
    #                     play_response = self.send_command("PLAY U0")
    #                     if "OK" not in play_response:
    #                         print("PLAY command failed with response:", play_response)
    #                         return False

    #                     # Get the next two GUIDs in the list, if available
    #                     if i + 1 < len(guids):
    #                         next_queued_guid = guids.iloc[i + 1]
    #                     if i + 2 < len(guids):
    #                         next_next_queued_guid = guids.iloc[i + 2]

    #                     # Construct the response message
    #                     response_message = (
    #                         f"MLT,{running_inventory_details},NEXT-RUN,{next_queued_guid},{next_next_queued_guid},LOCALMLT^_^DIRECT-PLAY^_^ACK"
    #                     )
    #                     print(response_message)
    #                     return response_message

    #         return True
    #     except Exception as ex:
    #         print("Error sending inventory commands:", ex)
    #         return "MLT^_^DIRECT-PLAY^_^NACK^_^ERROR"

    #okay running
    def send_inventory_commands(self, df_manager, clip_to_play_guid):
        try:
            df = df_manager.get_dataframe()
            
            guids = df[
                "GUID"
            ]  # Assuming 'GUID' is the name of the column containing GUIDs

            # Flag to indicate when to start appending inventory items
            start_appending = False

            for guid in guids:
                # If the current GUID matches the specified GUID, start appending items
                if guid == clip_to_play_guid:
                    start_appending = True

                if start_appending:
                    # Assuming inventory items are filenames, you may need to adjust this part
                    inventory_item = df[df["GUID"] == guid]["Inventory"].iloc[0]
                    rows_count= df.get_num_rows()
                    command = f"APND U0 {inventory_item}.mp4"
                    duration = df[df["GUID"] == guid]["Duration"].iloc[0]
                    response = self.send_command(command)

                    # if "OK" not in response:
                    #     print(f"Command {command} failed with response: {response}")
                    #     return False

                    # After the first inventory item is appended, send the PLAY command
                    if (
                        inventory_item
                        == df[df["GUID"] == clip_to_play_guid]["Inventory"].iloc[0]
                        
                    ):
                        play_response = self.send_command("PLAY U0")

                        global_context.set_value('onairmsg',)
                        global_context.set_value('readymsg',)
                        global_context.set_value('response',f"{inventory_item}|{duration}|{time.get_frame_from_time(duration)}")    #clipid|frames played | total frames
                        global_context.set_value('elapsed',time.get_frame_from_time(duration))
                        global_context.set_value('rowscount', rows_count)
                        global_context.set_value('FirstRow',)
                        global_context.set_value('LastRow',)
                        
                        if "OK" not in play_response:
                            print("PLAY command failed with response:", play_response)
                            return False
                        
                        else :
                          global_context = GlobalContext()
                          global_context.set_value('response',{})


            return True
        except Exception as ex:
            print("Error sending inventory commands:", ex)
            return False

    def start(self, GUID):
        try:
            if self.start_melted_server():
                global_context = GlobalContext()
                df_manager = DataFrameManager()
                # df_manager.load_dataframe(filepath)
                df_manager.get_dataframe()

                clip_to_play = df_manager.get_clipid_by_guid(GUID)

                if self.send_initial_commands():
                    response =self.send_inventory_commands(df_manager, GUID)
                    return response
        except Exception as ex:
            print("Error:", ex)
        # finally:
        #     if self.connected:
        #         self.client_socket.close()
        #         # print("Connection closed.")

    def delete_entry_by_guid(self, GUID):
        try:
            df_manager = DataFrameManager()
            df = df_manager.get_dataframe()

            # Check if the GUID exists in the DataFrame
            if GUID in df["GUID"].values:
                # Remove the row with the matching GUID
                df = df[df["GUID"] != GUID]
                df_manager._df = (
                    df  # Update the DataFrame in the DataFrameManager instance
                )

                print(f"Entry with GUID {GUID} deleted.")
                return f"MLT^_^DELETE^_^{GUID}"
            else:
                print(f"GUID {GUID} not found.")
                return "MLT^_^DELETE^_^NACK^_^GUID_NOT_FOUND"
        except Exception as ex:
            print("Error deleting entry:", ex)
            return "MLT^_^DELETE^_^NACK^_^ERROR"

    def insert_entry(self, command):
        try:
            # Parse the command
            parts = command.split("^_^")
            GUID = parts[2]
            data = parts[3].split(",")

            # Extract the position to insert
            position = int(data[0]) - 1  # Adjusting position to 0-based index
            row_data = data[1:]

            # Create a new GUID for the new row
            new_guid = DataFrameManager.generate_guid()

            # Ensure the new GUID is placed in the GUID column
            row_data[17] = new_guid

            # Create a DataFrameManager instance and get the DataFrame
            df_manager = DataFrameManager()
            df = df_manager.get_dataframe()

            # Convert row data into a DataFrame row
            new_row = pd.DataFrame([row_data], columns=df.columns)

            # Insert the new row into the DataFrame at the specified position
            df1 = pd.concat(
                [df.iloc[:position], new_row, df.iloc[position:]]
            ).reset_index(drop=True)
            df_manager._df = (
                df1  # Update the DataFrame in the DataFrameManager instance
            )

            # Convert the new row to a comma-separated string
            new_row_str = f"{position}," + ",".join(
                new_row.iloc[0].astype(str).tolist()
            )

            print(f"Entry inserted at position {position}.")
            return f"MLT^_^INSERT^_^{GUID}^_^{new_row_str}"
        except Exception as ex:
            print("Error inserting entry:", ex)
            return "MLT^_^INSERT^_^NACK^_^ERROR"

    def paste_entry(self, command):
        try:
            # Parse the command to extract GUIDs
            parts = command.split("^_^")
            source_guid = parts[3]
            target_guid = parts[2]

            # Create a DataFrameManager instance and get the DataFrame
            df_manager = DataFrameManager()
            df = df_manager.get_dataframe()

            # Find the row to copy using source_guid
            source_row = df.loc[df["GUID"] == source_guid]

            if not source_row.empty:
                source_row_data = source_row.iloc[0].astype(str).tolist()

                # Generate a new GUID for the new row
                new_guid = DataFrameManager.generate_guid()
                source_row_data[17] = (
                    new_guid  # Assuming the GUID column is at index 15
                )

                # Find the position to insert the new row using target_guid
                target_row = df.loc[df["GUID"] == target_guid]
                if not target_row.empty:
                    target_index = target_row.index[0]

                    # Create the new row DataFrame
                    new_row_df = pd.DataFrame([source_row_data], columns=df.columns)

                    # Insert the new row just above the target row
                    df1 = pd.concat(
                        [df.iloc[:target_index], new_row_df, df.iloc[target_index:]]
                    ).reset_index(drop=True)
                    df_manager._df = (
                        df1  # Update the DataFrame in the DataFrameManager instance
                    )

                    print(
                        f"Row copied from GUID {source_guid} to new GUID {new_guid} above GUID {target_guid}."
                    )
                    return f"MLT^_^PASTE^_^{source_guid}^_^{target_guid}<->{new_guid}"
                else:
                    return f"MLT^_^PASTE^_^{source_guid}^_^{target_guid}^_^NACK^_^ERROR: Target row not found"
            else:
                return f"MLT^_^PASTE^_^{source_guid}^_^{target_guid}^_^NACK^_^ERROR: Source row not found"
        except Exception as ex:
            print("Error pasting entry:", ex)
            return f"MLT^_^PASTE^_^{source_guid}^_^{target_guid}^_^NACK^_^ERROR"

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

    def send_packet_details(self, df_manager, current_guid):
        try:
            df = df_manager.get_dataframe()
            current_index = df.index[df["GUID"] == current_guid][0]

            # Get current row details
            current_row = df.iloc[current_index]
            current_details = (
                f"{current_row['EventTime']}~{current_row['Inventory']}~{current_row['Title']}~"
                f"{current_row['Reconcile']}~{current_row['SOM']}~{current_row['Duration']}"
            )

            # Get next row details
            if current_index + 1 < len(df):
                next_row = df.iloc[current_index + 1]
                next_details = (
                    f"{next_row['EventTime']}~{next_row['Inventory']}~{next_row['Title']}~"
                    f"{next_row['Reconcile']}~{next_row['SOM']}~{next_row['Duration']}"
                )
                next_guid = next_row['GUID']
            else:
                next_details = next_guid = ""

            # Get next-next row details if available
            if current_index + 2 < len(df):
                next_next_row = df.iloc[current_index + 2]
                next_next_guid = next_next_row['GUID']
            else:
                next_next_guid = ""

            # Construct the packet
            Packet = (
                f"MLT,{current_details},{next_details},{current_row['Status']},"
                f"NEXT-RUN,{current_row['GUID']},{next_guid},{next_next_guid}"
            )

            # Send the packet via TCP connection
            self.send_to_tcp(Packet)

            return Packet
        except Exception as ex:
            print("Error sending packet details:", ex)
            return None

    def send_to_tcp(self, message):
        try:
            # Configure your TCP connection details here
            tcp_ip = '127.0.0.1'
            tcp_port = 5005
            buffer_size = 1024

            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((tcp_ip, tcp_port))
            s.sendall(message.encode())
            s.close()
        except Exception as ex:
            print("Error sending to TCP:", ex)









































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
