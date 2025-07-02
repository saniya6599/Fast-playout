import re
import pandas as pd
from CommonServices.Global_context import GlobalContext
from CommonServices.DataFrameManager import DataFrameManager

class ReckonKeyGeneratorByPattern:
    def __init__(self, key_column: str = "Reconcile"):
        self.df = DataFrameManager()
        self.key_column = key_column
        self.global_context=GlobalContext()

    def generate(self, used_keys):
        
        df=self.df.get_dataframe()
        existing_keys = set(df[self.key_column].dropna().astype(str))

        pattern = r"^([^\d]*?)([-_]?)?(\d+)$"

        parsed_keys = []

        for key in existing_keys:
            match = re.match(pattern, key)
            if match:
                prefix, sep, num_str = match.groups()
                num = int(num_str)
                parsed_keys.append((prefix, sep, num, len(num_str)))

        if not parsed_keys:
            return f"{self.global_context.get_value('channel_name')}-0001"  # Fallback default if nothing matched

        # Group keys by prefix & separator to find the most used format
        from collections import Counter
        format_counter = Counter((p[0], p[1], p[3]) for p in parsed_keys)
        common_format = format_counter.most_common(1)[0][0]  # (prefix, sep, pad_len)

        prefix, sep, pad_len = common_format

        # Find max number for the most common format
        max_num = max(p[2] for p in parsed_keys if (p[0], p[1], p[3]) == common_format)

        # Try generating next unique key
        while True:
            max_num += 1
            new_key = f"{prefix}{sep}{str(max_num).zfill(pad_len)}" if sep else f"{prefix}{str(max_num).zfill(pad_len)}"
            if new_key not in (existing_keys and used_keys):
                return new_key
            
            
        
        
        
        
        
        
        
        
        
        # df= self.df.get_dataframe()
        # existing_keys = df[self.key_column].dropna().astype(str).tolist()
        # if not existing_keys:
        #     # fallback if no keys
        #     return f"{self.global_context.get_value('channel_name')}-0001"

        # last_key = existing_keys[-1]  # use last as base
        #  # Match various formats: PMSL-0010, B2200414, 796149848
        # match = re.match(r"^([^\d]*?)([-_]?)?(\d+)$", last_key)

        # if not match:
        #     raise ValueError(f"Unrecognized format: {last_key}")
        #     return last_key + "-001"

        # prefix, separator, numeric_part = match.groups()
        # # new_num = str(int(num_part) + 1).zfill(len(num_part))  # preserve digit length
        # new_number = int(numeric_part) + 1
        # padded_number = str(new_number).zfill(len(numeric_part))
        # return f"{prefix}{separator}{padded_number}" if separator else f"{prefix}{padded_number}"