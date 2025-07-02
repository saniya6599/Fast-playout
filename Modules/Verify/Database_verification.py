import json
import os
import subprocess
import pandas as pd
import requests
from CommonServices.DataFrameManager import DataFrameManager
from CommonServices.Global_context import GlobalContext
from flask import Flask, request, jsonify
from CommonServices.Logger import Logger

class Database_verification:
    def __init__(self):
        self.global_context = GlobalContext()
        self.df_manager = DataFrameManager()
        self.df_manager.get_dataframe()
        self.logger=Logger()
        
        
    def verify(self):
        try:

            df = self.df_manager.get_dataframe()

            # Extract unique 'Inventory' values
            if 'Inventory' in df.columns:
                filtered_df = df[
                    df['Type'].str.lower().isin(['pri', 'primary']) &  
                    ~df['Inventory'].str.startswith('LIVE', na=False)
                ]
                 # Extract unique 'Inventory' values from the filtered data
                unique_inventory = filtered_df['Inventory'].dropna().unique().tolist()
                
                self.logger.info(f"Unique inventory :{unique_inventory}")
                for inventory_id in unique_inventory[:]:
                    clip_ext= self.get_file_extension(inventory_id)
                    clip_path = f"{os.path.join(self.global_context.get_value('ContentLoc'), inventory_id)}{clip_ext}"
                    self.logger.info(f"Checking ID if present : {inventory_id}")
                    if not os.path.exists(clip_path):  
                        # If clip exists, update status to "O.K"
                        self.logger.info(f"Missing : {inventory_id}")
                        unique_inventory.remove(inventory_id)
                        row_indexes = df[df['Inventory'] == inventory_id].index
                        
                        for row_index in row_indexes:
                         self.df_manager.update_row(row_index, 'Status', "N.A")
                         
                print(f"Filtered Inventory values: {unique_inventory}")
            else:
                print("Column 'Inventory' not found in DataFrame")
                return

            # Prepare payload for POST request
            payload = {"channelid":f"{self.global_context.get_value('channel_id')}","inventory": unique_inventory}
            
            # URL for the API
            url = "http://127.0.0.1:8050/get_vsview"

            self.logger.info(f"Payload for database verification : {payload} at endpoint : {url}")
            
            # Send POST request
            # response = requests.post(url, json=payload)
            response = requests.request("POST", url,json=payload)
            # self.logger.info(f"response for db verification : {response}")
            print(response)
            
            # Check response status
            if response.status_code == 200:

                result = response.json()
                # self.logger.info(f"Data returned by config manager : {result}")
                
                self.update(result)
                
                if df is not None:
                    lines = self.df_manager.get_lines()
                    return lines
                else:
                    return lines

            else:
                self.logger.error(f"API call failed with status code: {response.status_code}")
                                
        #         result = [
        #     {
        #         "sno": 1,
        #         "tcrin": "00:00:00:00",
        #         "Region": "NOIDA",
        #         "clipid": "testmlttttttttttt",
        #         "status": "M",
        #         "tapeid": "COMM1",
        #         "tcrout": "",
        #         "duration": "00:00:00:00",
        #         "parentid": "testmlttttttttttt",
        #         "streamqc": "1",
        #         "Entrydate": "2024-11-08 11:03:31.000000",
        #         "Extention": ".mp4",
        #         "SubClipid": "",
        #         "eventtype": "PRI",
        #         "segmentno": "1",
        #         "AspectRatio": "4:3",
        #         "Description": "",
        #         "eventdetails": "",
        #         "SegmentDuration": "02:00:00:00"
        #     },
        #     {
        #         "sno": 25,
        #         "tcrin": "10:14:35:24",
        #         "Region": "NOIDA",
        #         "clipid": "L11AND",
        #         "status": "M",
        #         "tapeid": "COMM2",
        #         "tcrout": "",
        #         "duration": "00:00:00:00",
        #         "parentid": "L11AND",
        #         "streamqc": "1,1",
        #         "Entrydate": "2024-11-13 17:45:24.000000",
        #         "Extention": ".mp4",
        #         "SubClipid": "",
        #         "eventtype": "PRI",
        #         "segmentno": "2",
        #         "AspectRatio": "16:9",
        #         "Description": "",
        #         "eventdetails": "",  # Missing comma added here
        #         "SegmentDuration": "03:00:00:00"
        #     },
        #     {
        #         "sno": 23,
        #         "tcrin": "00:14:05:08",
        #         "Region": "NOIDA",
        #         "clipid": "testmlttcmlt",
        #         "status": "M",
        #         "tapeid": "COMM3",
        #         "tcrout": "",
        #         "duration": "00:00:00:00",
        #         "parentid": "testmlttcmlt",
        #         "streamqc": "1",
        #         "Entrydate": "2024-11-13 16:26:28.000000",
        #         "Extention": ".mp4",
        #         "SubClipid": "",
        #         "eventtype": "PRI",
        #         "segmentno": "3",
        #         "AspectRatio": "4:3",
        #         "Description": "",
        #         "eventdetails": "",
        #         "SegmentDuration": "04:00:00:00"
        #     },
        #     {
        #         "sno": 23,
        #         "tcrin": "00:14:05:08",
        #         "Region": "NOIDA",
        #         "clipid": "testmlttcmlt",
        #         "status": "M",
        #         "tapeid": "COMM4",
        #         "tcrout": "",
        #         "duration": "00:00:00:00",
        #         "parentid": "testmlttcmlt",
        #         "streamqc": "1",
        #         "Entrydate": "2024-11-13 16:26:28.000000",
        #         "Extention": ".mp4",
        #         "SubClipid": "",
        #         "eventtype": "PRI",
        #         "segmentno": "4",
        #         "AspectRatio": "4:3",
        #         "Description": "",
        #         "eventdetails": "",
        #         "SegmentDuration": "05:00:00:00"
        #     }
        # ]
        #         self.update(result)
                
                if df is not None:
                    lines = self.df_manager.get_lines()
                    return lines
                else:
                    return lines
                
        except Exception as e:
            self.logger.error(f"Error in database verification: {e}")
          
    def verify_content(self,content):
        try:
            self.logger.info(f"Verifying content from database {content}")
            if content :
                df = self.df_manager.get_dataframe()

                unique_inventory = [content] if isinstance(content, str) else list(content or [])
                
                self.logger.info(f"Unique inventory :{unique_inventory}")
                for inventory_id in unique_inventory[:]:
                    clip_ext= self.get_file_extension(inventory_id)
                    clip_path = f"{os.path.join(self.global_context.get_value('ContentLoc'), inventory_id)}{clip_ext}"
                    self.logger.info(f"Checking ID if present : {inventory_id}")
                    if not os.path.exists(clip_path):  
                        # If clip exists, update status to "O.K"
                        self.logger.info(f"Missing : {inventory_id}")
                        unique_inventory.remove(inventory_id)
                        row_indexes = df[df['Inventory'] == inventory_id].index
                        
                        for row_index in row_indexes:
                         self.df_manager.update_row(row_index, 'Status', "N.A")
                         
                print(f"Filtered Inventory values: {unique_inventory}")
            else:
                self.logger.error(f"Unable to verify : {content}")
                return

            # Prepare payload for POST request
            payload = {"channelid":f"{self.global_context.get_value('channel_id')}","inventory": unique_inventory}
            
            # URL for the API
            url = "http://127.0.0.1:8050/get_vsview"

            # self.logger.info(f"Payload for database verification : {payload} at endpoint : {url}")
            
            # Send POST request
            # response = requests.post(url, json=payload)
            response = requests.request("POST", url,json=payload)
            # self.logger.info(f"response for db verification : {response}")
            print(response)
            
            # Check response status
            if response.status_code == 200:

                result = response.json()
                self.logger.info('Response : 200, Result : {result}')
                # self.logger.info(f"Data returned by config manager : {result}")
                
                self.update(result)
                
                if df is not None:
                    lines = self.df_manager.get_lines()
                    return lines
                else:
                    return lines

            else:
                self.logger.error(f"API call failed with status code: {response.status_code}")
                                
                if df is not None:
                    lines = self.df_manager.get_lines()
                    return lines
                else:
                    return lines
                
        except Exception as e:
            self.logger.error(f"Error in database verification: {e}")
    
            
    def update(self, result):
        # self.logger.info("Fetching db values...")

        if not isinstance(result, list):
            self.logger.error("Database verification update - Invalid input: 'result' must be a list of dictionaries.")
            return

        for record in result:
            try:
                if not isinstance(record, dict):
                    self.logger.error(f"Skipping invalid record: {record}")
                    continue

                required_keys = {"tapeid", "Extention", "tcrin", "clipid", "SegmentDuration", "eventtype", "segmentno"}
                if not required_keys.issubset(record.keys()):
                    self.logger.error(f"Skipping record due to missing keys: {record}")
                    continue

                tapeid = record.get("tapeid")
                extension = record.get("Extention")
                tcrin = record.get("tcrin")
                clipid = record.get("clipid")
                segment_duration = record.get("SegmentDuration")
                duration = record.get("duration")
                eventtype = record.get("eventtype")
                segmentno = record.get("segmentno")

                if not all([tapeid, extension, tcrin]):
                    self.logger.error(f"Skipping record due to missing mandatory values: {record} for tapeid: {tapeid}")
                    continue

                try:
                    df = self.df_manager.get_dataframe()
                    matching_rows = df[(df['Inventory'] == tapeid) & (df['Segment'] == segmentno)].index
                except Exception as e:
                    self.logger.error(f"Database verification update - Error fetching DataFrame: {str(e)}")
                    continue

                if matching_rows.empty:
                    self.logger.info(f"Database verification update - No matching rows found for TapeID: {tapeid} and segment: {segmentno}")
                    continue

                # self.logger.info(f"Matching rows: {list(matching_rows)}")

                for index in matching_rows:
                    try:
                        cliplocation = os.path.join(self.global_context.get_value("ContentLoc"), f"{tapeid}{extension}")

                        # if not os.path.exists(cliplocation):
                        #     self.df_manager.update_row(index, 'Status', "N.A")
                        #     self.logger.info(f"File not found at {cliplocation}. Marking Status as N.A for index {index}.")
                        #     continue

                        self.df_manager.update_row(index, 'SOM', tcrin)
                        self.df_manager.update_row(index, 'Duration', segment_duration)
                        self.df_manager.update_row(index, 'Type', eventtype)
                        self.df_manager.update_row(index, 'Segment', segmentno)
                        # self.df_manager.update_row(index, 'Status', "O.K")
                        
                        
                        # Update 'Segment' if segmentno is not empty/null, otherwise update 'Duration'
                        if segmentno and str(segmentno).strip():
                            self.df_manager.update_row(index, 'Duration', segment_duration)
                            # self.logger.info(f"Updated 'Segment' for index {index} with SegmentDuration: {segment_duration}.")
                        else:
                            self.df_manager.update_row(index, 'Duration', duration)
                            # self.logger.info(f"Updated 'Duration' for index {index} with Duration: {segment_duration}.")

                        # self.logger.info(f"Updated row {index} with TapeID: {tapeid}, SOM: {tcrin}, Duration: {segment_duration}, Type: {eventtype}, Segment: {segmentno}.")

                    except Exception as e:
                        self.logger.error(f"Error updating row {index}: {str(e)}")
            except Exception as e:
                self.logger.error(f"Unexpected error processing record {record}: {str(e)}")

    def get_file_extension(self, filename):
      for root, _, files in os.walk(self.global_context.get_value("ContentLoc")):
            for file in files:
                if file.startswith(filename) and file[len(filename):len(filename)+1] == ".":
                    return os.path.splitext(file)[1]
      return None