
import pandas as pd
import os
from CommonServices.DataFrameManager import DataFrameManager
from CommonServices.Global_context import GlobalContext
from CommonServices.Logger import Logger
 
logger=Logger()
global_context=GlobalContext()
df_manager=DataFrameManager()

 
def delete(command,client):
    try:
        playlist_name = command.split('^_^')[2]

        df = df_manager.get_dataframe()
        if df is None or df.empty:
            logger.warning("Global DataFrame is empty. Nothing to delete from.")
            return f"{global_context.get_value('channel_name')}^_^DELETE^_^NACK^_^NO PLAYLIST LOADED"

        # Find all rows matching the playlist in ModeMain
        matched_df = df[df["ModeMain"] == playlist_name]

        if matched_df.empty:
            logger.warning(f"No entries with playlist = {playlist_name} found in DataFrame.")
            return f"{global_context.get_value('channel_name')}^_^DELETE^_^NACK^_^NO_MATCH"

        # Extract all GUIDs and join into a ~-separated string
        guids = matched_df["GUID"].tolist()
        guid_str = "~".join(guids)
        
        result = client.delete_entries_by_guids(guid_str)

        logger.info(f"GUIDs to delete for playlist {playlist_name}: {guid_str}")
        return result

    except Exception as ex:
        logger.error(f"Error processing playlist delete: {ex}", exc_info=True)
        return f"{global_context.get_value('channel_name')}^_^DELETE^_^NACK^_^ERROR"
