import json
import os
import subprocess
import pandas as pd
from CommonServices.DataFrameManager import DataFrameManager
from CommonServices.Global_context import GlobalContext

class PlaylistVerifier:
    def __init__(self):
        self.global_context = GlobalContext()
        self.df_manager = DataFrameManager()
        self.df_manager.get_dataframe()

    def full_playlist_verification(self):
        
      try:
        lines=[]
        # Get the global DataFrame
        df = self.df_manager.get_dataframe()
        
        # Check if df is None or empty
        if df is None or df.empty:
            return lines
        
        filtered_df = df[
                    df['Type'].str.lower().isin(['pri', 'primary']) &  
                    ~df['Inventory'].str.startswith('LIVE', na=False)
                ]

        # Get list of unique inventory IDs
        unique_inventory_ids = filtered_df["Inventory"].unique()
                
        metadata_list = []
        
        for inventory_id in unique_inventory_ids:
            # Get the row for the current inventory_id
            row = df[df["Inventory"] == inventory_id].iloc[0]
            guid = row["GUID"]
            
            # Fetch metadata using ffmpeg
            metadata = self.fetch_metadata(inventory_id)
            try:
                onair_index = int(self.global_context.get_value('previous_on_air_primary_index'))
            except (TypeError, ValueError):
                onair_index = 0
            
            # Store metadata in a list
            if metadata:
                metadata_list.append({"InventoryID": inventory_id, "Metadata": metadata,"GUID":guid, "Row": row})
            if not metadata :
                 # Locate rows using the InventoryID
                rows = df[df['Inventory'] == inventory_id]
                if not rows.empty:
                    # full_id= f"{inventory_id}{file_extension}"
                    cliplocation= os.path.join(self.global_context.get_value("ContentLoc"), inventory_id)
            
                    
                    for row_index in rows.index:
                        if row_index >= onair_index and not os.path.exists(cliplocation):
                            self.df_manager.update_row(row_index, 'Status', "N.A")

                    
                    # for row_index in rows.index:
                    #     if not os.path.exists(cliplocation):
                    #         self.df_manager.update_row(row_index, 'Status', "N.A")

            
        if metadata_list :
                self.verify_and_update(metadata_list)
         # Access the DataFrame
        df = self.df_manager.get_dataframe()
        if df is not None:
            lines = self.df_manager.get_lines()
            return lines
        else:
            return lines
      except Exception as e:
        print(f"Error in full_playlist_verification: {e}")
        return lines
    
    
    def fetch_metadata(self, inventory_id):
        cliplocation= os.path.join(self.global_context.get_value("ContentLoc"), inventory_id)
        # Construct the ffmpeg command
        file_extension= self.get_file_extension(inventory_id)
        if(file_extension is None) :
            return False
        command = f"ffmpeg -i {cliplocation}{file_extension} -f ffmetadata"
        
        try:
            # Run the command and capture the output
            result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            # Check if the command was successful
            if result.returncode == 1:
                # Parse the output to extract required metadata
                metadata = self.parse_ffmpeg_output(result.stderr)
                return metadata
            else:
                print(f"Error fetching metadata for {cliplocation}: {result.stderr}")
                return None
        except Exception as e:
            print(f"Exception occurred while fetching metadata for {cliplocation}: {e}")
            return None


    def parse_ffmpeg_output(self, output):
        metadata = {}

        lines = output.split('\n')
        for line in lines:
            line = line.strip()  # Remove leading and trailing spaces
            if line.startswith('Metadata'):
                # Metadata line, skip it
                continue
            elif line.startswith('Duration'):
                duration_str = line.split(',')[0].split(': ')[1].strip()
                som = line.split(',')[1].split(': ')[1].strip()
                metadata['duration'] = self.convert_to_hhmmssff(duration_str,is_duration=True)
                metadata['SOM'] = self.convert_to_hhmmssff(som,is_duration=False)
            elif line.startswith('Stream #0:0'):
                parts = line.split(', ')
                resolution_str = ""#parts[2].split(' ')[2]  # Extract resolution string
            #    # Remove any extra characters such as '[' and ':'
            #     width_height_str = resolution_str.split('[')[0].split(':')[0]
            #     width, height = map(int, width_height_str.split('x'))  # Convert width and height to integers
                metadata['resolution'] = resolution_str
            elif line.startswith('Input #0'):
                # Extracting the file extension
                filename = line.split(' ')[-1].strip("'")
                metadata['file_extension'] = f".{(filename.split('.')[-1])[0:3]}"

        return metadata
    

    def verify_and_update(self, metadata_list):
        df = self.df_manager.get_dataframe()

        for metadata in metadata_list:
            inventory_id = metadata['InventoryID']
            guid = metadata['GUID']

            # Locate rows using the InventoryID
            rows = df[df['Inventory'] == inventory_id]
            
            #check file existance
            file_extension= self.get_file_extension(inventory_id)
            
            if not rows.empty:
                for row_index in rows.index:
                    # Update Duration if different
                    if df.at[row_index, 'Duration'] != metadata['Metadata']['duration']:
                        self.df_manager.update_row(row_index, 'Duration', metadata['Metadata']['duration'])
                        
                     # Update Server Duration if different
                    if df.at[row_index, 'ServerDuration'] != metadata['Metadata']['duration']:
                        self.df_manager.update_row(row_index, 'ServerDuration', metadata['Metadata']['duration'])

                    # Update SOM if different
                    if df.at[row_index, 'SOM'] != metadata['Metadata']['SOM']:
                        self.df_manager.update_row(row_index, 'SOM', metadata['Metadata']['SOM'])

                    # Update VerifyStatus if different
                    if df.at[row_index, 'VerifyStatus'] != metadata['Metadata']['file_extension']:
                        self.df_manager.update_row(row_index, 'VerifyStatus', metadata['Metadata']['file_extension'])
                        
                    # self.df_manager.update_row(row_index, 'Status', "O.K")
                        
                   

    def convert_to_hhmmssff(self, time_str, is_duration):
        if is_duration:
            # For duration in HH:mm:ss.ss format
            hours, minutes, seconds = map(float, time_str.split(':'))
            total_seconds = hours * 3600 + minutes * 60 + seconds
        else:
            # For SOM in seconds format
            total_seconds = float(time_str)

        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = int(total_seconds % 60)
        frames = int((total_seconds - int(total_seconds)) * 25)  # Assuming 25 fps

        return f'{hours:02}:{minutes:02}:{seconds:02}:{frames:02}'

    def get_file_extension(self, filename):
      for root, _, files in os.walk(self.global_context.get_value("ContentLoc")):
            for file in files:
                if file.startswith(filename) and file[len(filename):len(filename)+1] == ".":
                    return os.path.splitext(file)[1]
      return None



    # def fetch_metadata(self, inventory_id):
    #     # Construct the ffmpeg command
    #     cliplocation= os.path.join(self.global_context.get_value("ContentLoc"), inventory_id)
    #     ffmpeglocation=os.path.join(self.global_context.get_value("ffmpeg"), 'ffmpeg')
    #     command = [
    #         "ffmpeg",
    #         "-i", f"{cliplocation}.mp4",
    #         "-hide_banner",
    #         "-loglevel", "error"
    #     ]
        
    #     try:
    #         # Run the command and capture the output
    #         result = subprocess.run(command, stderr=subprocess.PIPE, text=True)
    #         output = result.stderr
            
    #         som = "00:00:00.00"  # Assuming SOM is always 00:00:00.00 for simplicity
    #         duration = None
    #         resolution = None
    #         file_extension = inventory_id.split('.')[-1]

    #         for line in output.splitlines():
    #             if 'Duration' in line:
    #                 duration = line.split(',')[0].split()[1].strip()
    #             if 'Stream' in line and 'Video' in line:
    #                 res_info = line.split(',')[2].strip().split()[0]
    #                 resolution = res_info

    #         return {
    #             'FilePath': inventory_id,
    #             'SOM': som,
    #             'Duration': duration,
    #             'FileExtension': file_extension,
    #             'Resolution': resolution
    #         }
    #     except Exception as e:
    #         print(f"Exception occurred while fetching metadata for {inventory_id}: {e}")
    #         return None
