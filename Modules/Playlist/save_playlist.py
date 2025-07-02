import csv
import uuid
import pandas as pd
import os
from CommonServices.DataFrameManager import DataFrameManager
from CommonServices.Global_context import GlobalContext
from CommonServices.Logger import Logger
 
logger=Logger()
global_context=GlobalContext()
df_manager=DataFrameManager()
 


def save_part_playlist(command):
    try:
        data=command.split('^_^')
        guids = data[2].split('~')
        filename = f"{data[3]}.csv"
        if len(guids) != 2:
            logger.warning("Invalid input. Expected two GUIDs separated by '~'.")
            return

        start_guid, end_guid = guids[0], guids[1]

        column_names = [
            "Type", "EventTime", "Inventory", "Title", "Segment", "StartType",
            "Reconcile", "SOM", "Duration", "ServerDuration", "Status", "LiveStatus",
            "ScaleType", "VerifyStatus", "DateTime", "ModeMain", "ModeProtect",
            "GUID", "Category", "TempMedia", "PlayType"
        ]

        df = df_manager.get_dataframe()
        if df is None or df.empty:
            logger.warning("Global DataFrame is empty. Cannot write playlist.")
            return

        if start_guid not in df["GUID"].values or end_guid not in df["GUID"].values:
            logger.warning("One or both GUIDs not found in DataFrame.")
            return

        start_index = df[df["GUID"] == start_guid].index[0]
        end_index = df[df["GUID"] == end_guid].index[0]

        if start_index > end_index:
            logger.warning("Start GUID comes after End GUID. Swapping them.")
            start_index, end_index = end_index, start_index

        sliced_df = df.iloc[start_index:end_index + 1]
        sliced_df = sliced_df[column_names]

        sliced_df.loc[:, "GUID"] = ""

        playlist_dir = global_context.get_value("PlaylistLoc")



        
        file_path = os.path.join(playlist_dir, filename)
        sliced_df.to_csv(file_path, index=False, header=False)


        logger.info(f"playlist saved to: {file_path}")

    except Exception as e:
        logger.error(f"Exception in save_playlist: {e}", exc_info=True)


def save_full_playlist(command):
    try:
        data=command.split('^_^')
        filename = f"{data[2]}.csv"
        

        column_names = [
            "Type", "EventTime", "Inventory", "Title", "Segment", "StartType",
            "Reconcile", "SOM", "Duration", "ServerDuration", "Status", "LiveStatus",
            "ScaleType", "VerifyStatus", "DateTime", "ModeMain", "ModeProtect",
            "GUID", "Category", "TempMedia", "PlayType"
        ]

        df = df_manager.get_dataframe()
        if df is None or df.empty:
            logger.warning("Global DataFrame is empty. Cannot write playlist.")
            return

        df = df[column_names]

        df.loc[:, "GUID"] = ""

        playlist_dir = global_context.get_value("PlaylistLoc")

        file_path = os.path.join(playlist_dir, filename)
        df.to_csv(file_path, index=False, header=False)


        logger.info(f"playlist saved to: {file_path}")

    except Exception as e:
        logger.error(f"Exception in save_playlist: {e}", exc_info=True)
