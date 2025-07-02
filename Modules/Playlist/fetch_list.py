import csv
import glob
import uuid
import os
import pandas as pd
import json
from CommonServices.DataFrameManager import DataFrameManager
from CommonServices.Global_context import GlobalContext
# from Modules.melted.MeltedClient import MeltedClient


# def fetch_list():
#     df_manager = DataFrameManager()
#     global_context = GlobalContext()
    
#     if df_manager.get_dataframe() is not None:  # Check if the DataFrame is already loaded
#         entry = df_manager.get_lines()
#         # global_context.set_value('automsg','automation_stop')
#         rows_count=len(df_manager._df)
#         global_context.set_value("rows_count",rows_count)
#         return entry
#     else:
#         # global_context.set_value('automsg','automation_stop_linkedlist_empty')
#         return []  # No lines if DataFrame is not loaded
        
        
def fetch_list():
    df_manager = DataFrameManager()
    global_context = GlobalContext()

    df = df_manager.get_dataframe()
    if df is None or df.empty:
        return []

    try:
        # if(global_context.get_value('sync') == 1):
        #     onair_guid = global_context.get_value("onair_guid")
        #     onair_index = df[df['GUID'] == onair_guid].index[0]

        #     # Show last 10 played, on-air, and rest upcoming
        #     start_index = max(0, onair_index - 5)
        #     filtered_df = df.iloc[start_index:]

        #     global_context.set_value("rows_count", len(df))
        #     return df_manager.df_to_json(filtered_df)  # or your method to serialize
        # else :
        if df_manager.get_dataframe() is not None:  # Check if the DataFrame is already loaded
                entry = df_manager.get_lines()

                rows_count=len(df_manager._df)
                global_context.set_value("rows_count",rows_count)
                return entry
        else:
                 # global_context.set_value('automsg','automation_stop_linkedlist_empty')
                return []  # No lines if DataFrame is not loaded
            

    except Exception as e:
        print(f"Error in fetch_list: {e}")
        return []

   
     
        
        
        
        
    #    on_air_guid = global_context.get_value("onair_guid")
    #     if on_air_guid and on_air_guid in df["GUID"].values:
    #         current_index = df[df["GUID"] == on_air_guid].index[0]
    #         if current_index < 3:
    #             sliced_df = df
    #         else:
    #             start_index = current_index - 2
    #             end_index = current_index + 1 
    #             sliced_df = df.iloc[start_index:]
    #             # melted_command = f"REMOVE U0 0"
    #             # self.send_command(melted_command)        
    #     else:
    #         sliced_df = df
        
        
    #     # Generate CSV-style string lines
    #     rows_count=len(sliced_df)
    #     global_context.set_value("rows_count",rows_count)
    #     csv_data = sliced_df.to_csv(index=False, header=False, lineterminator='\r\n', na_rep='')
    #     lines = csv_data.splitlines()
    #     return [f"{index + 1},{line}" for index, line in enumerate(lines)]
        
    # else:
    #     # global_context.set_value('automsg','automation_stop_linkedlist_empty')
    #     return []  # No lines if DataFrame is not loaded
    



    

