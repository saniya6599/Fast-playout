import datetime
import os
import pandas as pd
import uuid
import datetime
from CommonServices.Logger import Logger
from CommonServices.Time_management import timemgt
from CommonServices.Global_context import GlobalContext


class DataFrameManager:
    _instance = None
    _df = None

    def __init__(self):
        self.logger=Logger()
        self.time=timemgt()
        self.global_context= GlobalContext()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DataFrameManager, cls).__new__(cls)
        return cls._instance

    @staticmethod
    def generate_guid():
        return str(uuid.uuid4())

    def load_dataframe(self, file_path,start_index=None):
        column_names = [
            "Type",
            "EventTime",
            "Inventory",
            "Title",
            "Segment",
            "StartType",
            "Reconcile",
            "SOM",
            "Duration",
            "ServerDuration",
            "Status",
            "LiveStatus",
            "ScaleType",
            "VerifyStatus",
            "DateTime",
            "ModeMain",
            "ModeProtect",
            "GUID",
            "Category",
            "TempMedia",
            "PlayType"
            #"Extra"
        ]


        try:
            self._df = pd.read_csv(
                file_path,
                header=None,
                names=column_names,
                skipinitialspace=True,
                na_filter=False,
                skip_blank_lines=True,
                skiprows=start_index if start_index not in (None, '',-1) else 0
            )
            self._df["GUID"] = self._df["GUID"].apply(
                lambda x: self.generate_guid() if pd.isna(x) or x == "" else x
            )

            # Set LiveStatus to "RD" where Inventory starts with "LIVE"
            self._df["LiveStatus"] = self._df.apply(
            lambda row: "RD" if row["Inventory"].startswith("LIVE") else row["LiveStatus"],
            axis=1
            )
            
            # Set the playlist column by extracting the filename from file_path
            playlist_name = os.path.basename(file_path)
            self._df["playlist"] = playlist_name
            # self._df["playlist_index"] = range(len(self._df))
            
            
            
            print(f"[INFO] DataFrame loaded successfully from {file_path}.")
            self.logger.info(f"DataFrame loaded successfully from {file_path}.")
        except FileNotFoundError:
                print(f"[ERROR] Playlist not found: {file_path}")
                self.logger.error(f"Playlist not found: {file_path}")
        except Exception as e:
                print(f"[ERROR] An error occurred while loading the DataFrame: {e}")
                self.logger.info(f"An error occurred while loading the DataFrame: {e}")


    def get_dataframe(self):
        return self._df

    def update_row(self, index, column, value):
        if self._df is not None:
            self._df.at[index, column] = value

    def get_lines(self):
     """Generate lines from the DataFrame as a list of strings, excluding column names and starting index from 1."""
     csv_data = self._df.to_csv(index=False, header=False, lineterminator='\r\n', na_rep='')
     lines = csv_data.splitlines()
     return [f"{index + 1},{line}" for index, line in enumerate(lines)]

    def check_playlist(self,playlist_folder, filename):
        file_path = os.path.join(playlist_folder, filename)

        if os.path.exists(file_path):
            # return f"ACK/LOAD-PLAYLIST/{filename}"
            return True
        else:
            # return "File does not exist"
            return False

    def get_clipid_by_guid(self, guid):
        if self._df is not None:
            row = self._df.loc[self._df['GUID'] == guid]
            if not row.empty:
                return row.iloc[0]['Inventory']
        return None



    def find_index_by_reckon_key(self, file_path, reckon_key):
       
        try:
            # Only load the 'Reconcile' column to save memory
            df = pd.read_csv(file_path, header=None, usecols=[6], skip_blank_lines=True)

            match = df[df[6] == reckon_key]
            if not match.empty:
                return match.index[0] 
            else:
                return -1  # No match found

        except Exception as e:
            print(f"[ERROR] Failed to find reckon key index: {e}")
            return -1






    def get_num_rows(self):
        if self._df is not None:
            return self._df.shape[0]
        return 0


    def toggle_up_dataframe(self):
        if self._df is None or self._df.empty:
            print("DataFrame is empty or not loaded.")
            return

        def parse_time_to_frames(time_str):
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

        self._df=self.get_dataframe()
        # Filter rows where Type is 'PRI' or 'PRIMARY' and status is not 'NA' (Missing content)
        filtered_df = self._df[self._df['Type'].str.lower().isin(['pri', 'primary'])]
                    #&
                    #~self._df['Status'].str.startswith('N.A', na=False)]

        if filtered_df.empty:
            print("No rows with Type 'PRI' or 'PRIMARY' found.")
            return

        # Get the current system time as the starting frame count

        # current_time = datetime.datetime.now()
        # current_frames = (current_time.hour * 3600 + current_time.minute * 60 + current_time.second) * 25 + current_time.microsecond // 40000

        current_time = timemgt.get_system_time_ltc()
        current_frames=parse_time_to_frames(current_time)

        if self.global_context.get_value("previous_on_air_primary_index") != "" :
            filtered_df =filtered_df.iloc[int(self.global_context.get_value("previous_on_air_primary_index"))+1:]

        for index, row in filtered_df.iterrows():
            
            status= str(row.get("Status", "")).strip().upper()
            # self._df.at[row.name, 'EventTime'] = frames_to_time_format(current_frames)
            self._df.at[row.name, 'EventTime'] = frames_to_time_format(current_frames)

            
            if status.startswith("N.A") :
                duration_frames=0
            else :
            # Calculate the duration of the current row in frames
                duration_frames = parse_time_to_frames(row['Duration'])

            # Update the current frame count by adding the duration frames
            current_frames += duration_frames
        # self.global_context.set_value('sync',1)
        print("Toggled up DataFrame with onairtime values for Type 'PRI' or 'PRIMARY'.")

    def clear_dataframe(self):

        if self._df is not None:
            self._df = self._df.iloc[0:0]  # Clear the DataFrame, preserving the structure
            print("[INFO] DataFrame has been cleared.")
        else:
            print("[WARNING] DataFrame is already empty or not initialized.")

