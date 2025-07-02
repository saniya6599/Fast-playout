import json
import os
import subprocess
import pandas as pd
import requests
from CommonServices.DataFrameManager import DataFrameManager
from CommonServices.Global_context import GlobalContext
from flask import Flask, request, jsonify
from CommonServices.Logger import Logger

class secondary_verification:
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
                    df['Type'].str.lower().isin(['sec', 'secondary']) &  
                    ~df['Inventory'].str.startswith('SCTE', na=False)
                ]
                 # Extract unique 'Inventory' values from the filtered data
                unique_inventory = filtered_df['Inventory'].dropna().unique().tolist()
                print(f"Filtered Inventory values: {unique_inventory}")
            else:
                print("Column 'Inventory' not found in DataFrame")
                return

            payload = {"channelid":f"{self.global_context.get_value('channel_id')}","inventory": unique_inventory}
            
            url = "http://localhost:8050/get_secview"
            
            self.logger.info(f"Payload for database verification : {payload} at endpoint : {url}")

            # Send POST request
            # response = requests.post(url, json=payload)
            response = requests.request("POST", url,json=payload)
            self.logger.info(f"response for secondary verification : {response}")
            print(response)
            
            # Check response status
            if response.status_code == 200:

                result = response.json()
                self.logger.info(f"Data returned by config manager : {result}")
                
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
            self.logger.error(f"Error during secondary verification: {e}")
            
    def update(self,result):
        print("Fetching db values...")
        self.global_context.get_value("channel_name")
        for record in result:
            print(f"{record}")
            clipid = record.get("clipid")
            status = record.get("STATUS_MAIN")
            
            
            # Find rows in the DataFrame where 'Inventory' matches 'tapeid'
            df = self.df_manager.get_dataframe()
            matching_rows = df[df['Inventory'] == clipid.upper()].index

            for index in matching_rows:
                
                # cliplocation = os.path.join(self.global_context.get_value("ContentLoc"), f"{tapeid}{extension}")
                # if not os.path.exists(cliplocation):
                #     self.df_manager.update_row(index, 'Status', "N.A")
                #     continue  # Skip updating other fields for this row
                
                
                self.df_manager.update_row(index, 'Status', status)
                #self.df_manager.update_row(index, 'Inventory', clipid)
                # self.df_manager.update_row(index, 'Duration', segment_duration)
                # self.df_manager.update_row(index, 'Type', eventtype)
                # self.df_manager.update_row(index, 'Segment', segmentno)
                