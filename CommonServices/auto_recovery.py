import atexit
import json
import os
import time
import requests
import signal
import sys
import threading
from datetime import datetime
from CommonServices.Global_context import GlobalContext
from CommonServices.DataFrameManager import DataFrameManager
from CommonServices.Logger import Logger

class AutoRecovery: 
    def __init__(self):
        
        self.api_url = "http://127.0.0.1:9080/api/recovery"
        self.db_push_interval = 300
        self.global_context=GlobalContext()
        self.df_manager=DataFrameManager()
        self.logger = Logger()
       

    # def setup_signal_handler(self):
    #     """Sets up signal handlers for graceful shutdown."""
    #     signal.signal(signal.SIGINT, self.trigger_recovery)  # Handle Ctrl+C
    #     signal.signal(signal.SIGTERM, self.trigger_recovery)  # Handle termination signals
        
    def setup_exit_handler(self):
        """Setup handlers for both atexit and termination signals."""
        atexit.register(self.perform_recovery)  # Handle normal exits

        def signal_handler(sig, frame):
            print(f"Signal {sig} received, performing recovery...")
            self.perform_recovery()
            exit(0)  # Ensure the program terminates after handling the signal

        # Register signal handlers for termination signals
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    def perform_recovery(self):
        
        signum=0
        print(f"Application interrupted with signal {signum}. Initiating recovery...")
        self.recover_and_send_data()
        # sys.exit(0)  # Exit the application gracefully

    def recover_and_send_data(self):

        try:
            # Fetch the latest settings
            recovery_data = self.settings_provider()

            # Add a timestamp to the recovery data
            recovery_data['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Send recovery data to API
            self.send_to_api(recovery_data)

            # Save recovery data to file
            self.save_to_file(recovery_data)

            # Push recovery data to the database
            # self.push_to_database(recovery_data)

        except Exception as e:
            print(f"Error during recovery process: {e}")

    def send_to_api(self, recovery_data):
        """Send recovery data to the API endpoint."""
        try:
            response = requests.post(
                self.api_url,
                data=json.dumps(recovery_data),
                headers={"Content-Type": "application/json"}
            )
            if response.status_code == 200:
                print("Recovery data successfully sent to API.")
            else:
                print(f"Failed to send recovery data. Status code: {response.status_code}, Response: {response.text}")
        except Exception as e:
            print(f"Error sending recovery data to API: {e}")

    def save_to_file(self, recovery_data):

        try:
            file_path = f"{os.path.join(self.global_context.get_value('PlaylistLoc'),'recovery.json')}"
            with open(file_path, "a") as file:
                file.write(json.dumps(recovery_data) + "\n")
            print("Recovery data successfully written to file.")
        except Exception as e:
            print(f"Error writing recovery data to file: {e}")

    def write_recovery_playlist(self):
  
        try:
            column_names = [
                                "Type", "EventTime", "Inventory", "Title", "Segment", "StartType",
                                "Reconcile", "SOM", "Duration", "ServerDuration", "Status", "LiveStatus",
                                "ScaleType", "VerifyStatus", "DateTime", "ModeMain", "ModeProtect",
                                "GUID", "Category", "TempMedia", "PlayType"
                            ]
            df = self.df_manager.get_dataframe()
            if df is None or df.empty:
                self.logger.warning("Global DataFrame is empty. Cannot write recovery playlist.")
                return

            current_reckonkey = self.global_context.get_value('current_reckonkey')
            if not current_reckonkey:
                self.logger.warning("current_reckonkey is not set. Cannot slice recovery playlist.")
                return

            matches = df.index[df['Reconcile'] == current_reckonkey].tolist()
            if not matches:
                self.logger.warning(f"current_reckonkey '{current_reckonkey}' not found in DataFrame. Writing full DataFrame.")
                sliced_df = df.copy()
            else:
                start_index = matches[0]
                sliced_df = df.iloc[start_index:]
                
                
            sliced_df = sliced_df[column_names]
            sliced_df.loc[:, "GUID"] = ""
            # sliced_df["playlist"] = ""

            recovery_dir = os.path.join(self.global_context.get_value("PlaylistLoc"),'Recovery') or "./recovery"
            # Create the Recovery folder if it doesn't exist
            if not os.path.exists(recovery_dir):
                os.makedirs(recovery_dir)
            
            # recovery_dir = os.paself.global_context.get_value("PlaylistLoc") or "./recovery"
            
            timestamp = datetime.now().strftime("%Y_%m_%d_%H-%M-%S")
            filename = f"recovery-{timestamp}.csv"
            file_path = os.path.join(recovery_dir, filename)
            
            self.global_context.set_value("current_recovered_playlist",filename)

            sliced_df.to_csv(file_path, index=False,header=False)
            self.logger.info(f"Recovery playlist saved to: {file_path}")

        except Exception as e:
            self.logger.error(f"Exception in write_recovery_playlist: {e}", exc_info=True)
