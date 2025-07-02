
import pandas as pd
import os
from CommonServices.DataFrameManager import DataFrameManager
from CommonServices.Global_context import GlobalContext
from CommonServices.Logger import Logger
 
logger=Logger()
global_context=GlobalContext()
df_manager=DataFrameManager()
 
def delete(command):
    try:
        playlist_name = command.split('^_^')[2]
        playlist_dir = global_context.get_value("PlaylistLoc")
        file_path = os.path.join(playlist_dir, playlist_name)

        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Playlist deleted: {file_path}")
            return f"{global_context.get_value('channel_name')}^_^DELETE-PLAYLIST^_^ACK"
        else:
            logger.warning(f"Playlist not found: {file_path}")
            return f"{global_context.get_value('channel_name')}^_^DELETE^_^NACK^_^FILE_NOT_FOUND"

    except Exception as ex:
        logger.error(f"Error deleting playlist: {ex}", exc_info=True)
        return f"{global_context.get_value('channel_name')}^_^DELETE^_^NACK^_^ERROR"