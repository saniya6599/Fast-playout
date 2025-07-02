import csv
import glob
import uuid
import os
import pandas as pd
import json
from CommonServices.DataFrameManager import DataFrameManager
from CommonServices.Global_context import GlobalContext
from CommonServices.Logger import Logger

def fetch_content(command):
    df_manager = DataFrameManager()
    global_context = GlobalContext()
    logger=Logger()

    try:
        filename=command.split('^_^')[2]
        filepath=os.path.join(global_context.get_value('PlaylistLoc'),filename)
        
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                lines = f.read().splitlines()

            return lines 
        else:
            logger.warning(f"File not found: {filepath}")
            return []            

    except Exception as e:
        logger.error(f"Error in fetch_content: {e}")
        return []