import csv
import datetime
import glob
import uuid
import os
import pandas as pd
import json

import pytz
from CommonServices.DataFrameManager import DataFrameManager
from CommonServices.Logger import Logger


logger=Logger()

def get_playlist(PlaylistLoc,channelname):

    folder = PlaylistLoc
    playlists = list_csv_files(folder)
    # List to store the file information
    logger.info(f"Total playlist available : {len(playlists)}")
    file_info_list = []
    for playlist in playlists:
        # Get file name
        file_name = os.path.basename(playlist)

        # Count the number of lines in the file
        with open(playlist, "r") as file:
            row_count = sum(1 for row in file)

        # # Get file creation time in utc
        # file_creation_time = os.path.getctime(playlist)
        # # Format the creation time as a string with AM/PM
        # creation_time_str = pd.to_datetime(file_creation_time, unit='s').strftime('%m/%d/%Y %I:%M:%S %p')

        
        # Get IST time directly
        ist_time = datetime.datetime.fromtimestamp(os.path.getctime(playlist), pytz.timezone("Asia/Kolkata"))

        creation_time_str=ist_time.strftime('%d-%b-%Y %I:%M:%S %p')

       
        df_manager = DataFrameManager()
        #df_manager.load_dataframe(playlist)    : 28.05

        # Access the DataFrame
        df = df_manager.get_dataframe()

        # Append the formatted file information to the list
        file_info_list.append(f"{file_name}~{row_count}~{creation_time_str}")

       # Join the list into a single string with each file info separated by a newline character
    result_string = "\n".join(file_info_list) + "\n"
    final = f"{channelname}^_^GET-PLAYLIST^_^{result_string}^_^GET-PLAYLIST-END\n"
    

    return final


def list_csv_files(folder):
    """List all CSV files in the given folder."""
    return glob.glob(os.path.join(folder, "*.csv"))
