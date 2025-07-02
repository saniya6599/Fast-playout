import csv
import uuid
import pandas as pd
import os
from CommonServices.DataFrameManager import DataFrameManager
from CommonServices.Global_context import GlobalContext
from CommonServices.Logger import Logger
 
logger=Logger()
global_context=GlobalContext()
  
def load_playlist(PlaylistLoc, File_name, channelname):
    try:
        df_manager = DataFrameManager()
        if_exists = df_manager.check_playlist(PlaylistLoc, File_name)
        
        if if_exists:
            # Load the DataFrame from the specified file
            df_manager.load_dataframe(os.path.join(PlaylistLoc, File_name))
            
            # Access the DataFrame
            df = df_manager.get_dataframe()
            if df is not None:
                
                last_guid = df.iloc[-1]['GUID']
                global_context.set_value("last_guid", last_guid)
        
                lines = df_manager.get_lines()
                return lines
            else:
                return lines
        else:
            logger.error(f"{os.path.join(PlaylistLoc, File_name)} does not exists!")
            return f"{channelname}^_^LOAD-PLAYLIST^_^FAILED^_^ERROR: File does not exist"
    except Exception as ex:
        print("Error loading playlist:", ex)
        logger.error("Error while loading playlist : {File_name}")
        return f"{channelname}^_^LOAD-PLAYLIST^_^FAILED^_^ERROR: {str(ex)}"

















    # try:
    #     df_manager = DataFrameManager()
    #     IfExists = df_manager.check_playlist(PlaylistLoc, File_name)
    #     if IfExists:
    #         #df_manager.load_dataframe(os.path.join(PlaylistLoc, File_name))    28.05

    #         # Access the DataFrame
    #         df = df_manager.get_dataframe()
    #         print(df)
    #         return f"{channelname}^_^LOAD-PLAYLIST^_^DONE"
    #     else:
    #         return f"{channelname}^_^LOAD-PLAYLIST^_^FAILED"

    
    # except Exception as e:
    #     print("Error in load playlist class ", e)
    #     return None
