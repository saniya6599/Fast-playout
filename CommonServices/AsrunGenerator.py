import os
import time
import pandas as pd
import requests
from CommonServices.Global_context import GlobalContext
from CommonServices.DataFrameManager import DataFrameManager
from datetime import date
from CommonServices.Logger import Logger


class AsRunFormatter:
    def __init__(self):
        self.global_context = GlobalContext()
        self.df_manager = DataFrameManager()
        self.logger = Logger()
        self.written_guids = []
        self.api_queue=[]


        # Define each column width
        self.column_widths = {
            "ON-AIR": 8,
            "DATE/TIME": 13,
            "ID": 30,
            "S": 2,
            "TITLE": 32,
            "SOM": 12,
            "DURATION": 12,
            "STATUS": 19,
            "DEVICE": 2,
            "CH": 30,
            "RECONCILE": 12,
            "TYPE": 3
        }
        
        # Create the header dynamically based on column widths
        self.header = (
            "ON-AIR   DATE/TIME                 ID                S               TITLE                   SOM     DURATION       STATUS        DEVICE                  CH                  RECONCILE TYP"
        )

        self.separator = self.create_separator()

    def create_separator(self):
        separator = ""
        for column, width in self.column_widths.items():
            separator += "-" * width + " "
        return separator.strip()


    def filter_dataframe_by_guid(self, guid):
        try:
            # Find the index of the row with the specified GUID
            if 'GUID' in self.df.columns:
                guid_index = self.df.index[self.df['GUID'] == guid].tolist()
                
                if guid_index:
                    # Get the first occurrence index of the specified GUID
                    max_index = guid_index[0]
                    
                    # Filter the DataFrame up to the row with the specified GUID (inclusive)
                    filtered_df = self.df.iloc[:max_index + 1]
                    self.logger.info(f"Filtered DataFrame up to GUID {guid}.")
                    print(f"[INFO] Filtered DataFrame up to GUID {guid}.")
                    return filtered_df
                else:
                    self.logger.warning(f"GUID {guid} not found in the DataFrame.")
                    print(f"[WARNING] GUID {guid} not found in the DataFrame.")
                    return None
            else:
                self.logger.warning("Column 'GUID' not found in the DataFrame.")
                print("[WARNING] Column 'GUID' not found in the DataFrame.")
                return None
            
        except Exception as ex:
            self.logger.error(f"Exception in filter_dataframe_by_guid: {ex}")
            print(f"[EXCEPTION] {ex}")
            return None
        
    def format_line(self, row):
        try:
            # Format each row to fit the defined column widths
            formatted_line = ""
            formatted_line += self.format_column(date.today().strftime("%d-%m-%y"),self.column_widths["ON-AIR"])
            formatted_line += self.format_column(row['EventTime'], self.column_widths["DATE/TIME"])
            formatted_line += self.format_column(row['Inventory'], self.column_widths["ID"])
            formatted_line += self.format_column(row['Segment'], self.column_widths["S"])
            formatted_line += self.format_column(row['Title'], self.column_widths["TITLE"])
            formatted_line += self.format_column(row['SOM'], self.column_widths["SOM"])
            formatted_line += self.format_column(row['Duration'], self.column_widths["DURATION"])
            formatted_line += self.format_column(row['Status'], self.column_widths["STATUS"])
            formatted_line += self.format_column(row['ModeMain'], self.column_widths["DEVICE"])
            formatted_line += self.format_column(row['LiveStatus'], self.column_widths["CH"])
            formatted_line += self.format_column(row['Reconcile'], self.column_widths["RECONCILE"])
            formatted_line += self.format_column(row['ScaleType'], self.column_widths["TYPE"])
            return formatted_line
        except Exception as ex:
            self.logger.error(f"Exception in format_line: {ex}")
            print(f"[EXCEPTION] {ex}")
            return ""
        

    def format_column(self, value, width):
        try:
            if isinstance(value, str):
                return value.ljust(width)
            elif isinstance(value, (int, float)):
                return str(value).rjust(width)
            else:
                return " " * width
        except Exception as ex:
            self.logger.error(f"Exception in format_column: {ex}")
            print(f"[EXCEPTION] {ex}")
            return " " * width
    
    
    def write_to_file_until_guid(self, filename, guid):
        try:
            filtered_df = self.filter_dataframe_by_guid(guid)

            if filtered_df is not None:
                with open(filename, 'w') as file:
                    file.write(self.header + '\n')
                    
                    separator = '-' * len(self.header)
                    file.write(separator + '\n')
                    
                    for _, row in filtered_df.iterrows():
                        formatted_line = self.format_line(row)
                        file.write(formatted_line + '\n')
                
                # self.logger.info(f"File written successfully to {filename}")
                print(f"[INFO] File written successfully to {filename}")
            else:
                self.logger.warning("No data to write, GUID not found.")
                print("[WARNING] No data to write, GUID not found.")
        except Exception as ex:
            self.logger.error(f"Exception in write_to_file_until_guid: {ex}")
            print(f"[EXCEPTION] {ex}")
            
            
    # def run_asrun_formatter_periodically(self):
    #     try:    
    #         current_date=date.today().  strftime("%Y-%m-%d")
    #         self.logger.info(f"Running AsRunFormatter for date: {current_date}")
    #         print(f"[INFO] Running AsRunFormatter for date: {current_date}")
    #         while True:
    #             # print("Running AsRunFormatter to write the file...")
    #             self.write_to_file(os.path.join(self.global_context.get_value('asrun_location'),f"{self.global_context.get_value('channel_name')}-{current_date}"))
    #             time.sleep(60)  # Wait for 5 minutes (300 seconds)
    #     except Exception as ex:
    #             self.logger.error(f"Exception in run_asrun_formatter_periodically: {ex}")
    #             print(f"[EXCEPTION] {ex}")

        
    def run_asrun_formatter_periodically(self):
        try:    
            current_date = date.today().strftime("%Y-%m-%d")
            self.logger.info(f"Running AsRunFormatter for date: {current_date}")
            print(f"[INFO] Running AsRunFormatter for date: {current_date}")
            
            while True:
                self.write_to_file(os.path.join(self.global_context.get_value('asrun_location'),
                                                f"{self.global_context.get_value('channel_name')}-{current_date}"))
                self.send_batch_to_api()  # Send accumulated API data
                time.sleep(300)  # Wait for 5 minutes (300 seconds)

        except Exception as ex:
            self.logger.error(f"Exception in run_asrun_formatter_periodically: {ex}")
            print(f"[EXCEPTION] {ex}")    
    
    
    def send_batch_to_api(self):
        if not self.api_queue:
            # self.logger.info("No new asrun data to send.")
            print("[INFO] No new asrun data to send.")
            return

        url = "http://127.0.0.1:8050/post_asrunlog"
        try:
            response = requests.post(url, json=self.api_queue)  # Send batch data
            if response.status_code == 200:
                print(f"[INFO] Sent {len(self.api_queue)} asrun entries to post_asrunlog.")
                # self.logger.info(f"Sent {len(self.api_queue)} asrun entries to post_asrunlog.")
                self.api_queue.clear()  # Clear sent data
            else:
                print(f"[ERROR] post_asrunlog request failed with status: {response.status_code}")
                self.logger.error(f" post_asrunlog request failed with status: {response.status_code}")

        except Exception as ex:
            print(f"[EXCEPTION] Error in sending batch to post_asrunlog: {ex}")
            self.logger.error(f"Error in sending batch to post_asrunlog: {ex}")
        
        
    def write_to_file(self, filename):
     try:
        asrun_list = self.global_context.get_value("asrun_list")
        if not asrun_list or not isinstance(asrun_list, list):
            self.logger.info("No GUIDs available in 'asrun_list'.")
            print("[INFO] No GUIDs available in 'asrun_list'.")
            return


        asrun_list = [guid for guid in asrun_list if guid not in self.written_guids]
        df=self.df_manager.get_dataframe()
        filtered_df = df[df['GUID'].isin(asrun_list)]

        if filtered_df.empty:
            # self.logger.warning("No matching GUIDs found in the DataFrame.")
            print("[WARNING] No matching GUIDs found in the DataFrame.")
            return
        
        file_exists = os.path.exists(filename)                                                                                                                                                                                      

        with open(filename, 'a') as file:

                if not file_exists:
                    file.write(self.header + '\n')

                    # # Write a separator line (optional)
                    # separator = '-' * len(self.header)
                    file.write(self.separator + '\n')
                    

                # Write each row from the filtered DataFrame
                for _, row in filtered_df.iterrows():
                    formatted_line = self.format_line(row)
                    file.write(formatted_line + '\n')

                    row_json = self.create_row_json(row)
                    self.api_queue.append(row_json)
                    # self.send_data_to_api(row_json)
                    # Add the GUID to the written_guids list
                    self.written_guids.append(row['GUID'])

        # self.logger.info(f"File written successfully to {filename}")
        print(f"[INFO] File written successfully to {filename}")

     except Exception as ex:
            self.logger.error(f"Exception in write_to_file: {ex}")
            print(f"[EXCEPTION] {ex}")

    def send_data_to_api(self, payload):
      
        url = "http://127.0.0.1:8050/post_asrunlog"
        file_data=""
        try:
            response = requests.post(url, json=payload)
            # response.raise_for_status()
            if response.status_code == 200:
                # self.logger.info(f"Asrun payload sent successfully: {payload}")
                print(f"[INFO] Asrun payload sent successfully: {payload}")
            else:
                self.logger.error(f"Failed to send asrun payload. Status code: {response.status_code}")
                print(f"[ERROR] Failed to send asrun payload. Status code: {response.status_code}")
        except Exception as ex:
            self.logger.error(f"Exception in send_data_to_api: {ex}")
            print(f"[EXCEPTION] {ex}")
            
    def create_row_json(self, row):
        try:
            current_date=date.today().strftime("%Y-%m-%d")
            return {
                "Channel_Id": self.global_context.get_value("channel_id"),
                "EventType": row["Type"],
                "ClipId": row["Inventory"],
                "OnAirDate": current_date,
                "OnAirTime": row["EventTime"],
                "OnAirDateTime": f"{current_date} {row['EventTime']}",  # Full datetime
                "SOM": row["SOM"],
                "Duration": row["Duration"],
                "Status": row["Status"],
                "ReconKey": row["Reconcile"],
                "ClipTitle": row["Title"],
                "Segment": row["Segment"],
                "sno": "",  
                "EOM": row.get("EOM", ""), 
                "user": ""  
            }
        except Exception as ex:
            self.logger.error(f"Exception in create_row_json: {ex}")
            print(f"[EXCEPTION] {ex}")
            