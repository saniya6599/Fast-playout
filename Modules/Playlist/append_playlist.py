import os
import pandas as pd
from CommonServices.DataFrameManager import DataFrameManager
from CommonServices.Global_context import GlobalContext
from CommonServices.Logger import Logger

logger=Logger()

def append_playlist(Playlist, PlaylistDate):
    try:
        # Accessing the singleton instance
        global_context = GlobalContext()
        loc = global_context.get_value("PlaylistLoc")
        
        logger.info(f"Playlist location selected : {loc}")

        # Load the new DataFrame from the CSV file
        new_file_path = os.path.join(loc, Playlist)
        new_df = pd.read_csv(new_file_path, header=None, skipinitialspace=True, na_filter=False, skip_blank_lines=True)

        # Access the existing DataFrame
        df_manager = DataFrameManager()
        existing_df = df_manager.get_dataframe()

        if existing_df is None:
            # If existing DataFrame is None, load the new DataFrame directly
            df_manager._df = new_df
        else:
            # Ensure column names are consistent
            new_df.columns = existing_df.columns
             # Generate a GUID for each new row
            new_df['GUID'] = new_df['GUID'].apply(lambda x: df_manager.generate_guid())
            

            # Append the new DataFrame to the existing DataFrame
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)

            # Update the DataFrame in the DataFrameManager instance
            df_manager._df = combined_df

        # Return the combined DataFrame as lines
        return df_manager.get_lines()
    
    
        self.df_manager.toggle_up_dataframe()
        self.start_append_thread(df, guids,self.on_air_guid)
        self.start_update_thread()

    except FileNotFoundError:
        print("File not found.")
        return []
