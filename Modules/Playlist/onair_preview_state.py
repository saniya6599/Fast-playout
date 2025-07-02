
import pandas as pd
import json
from CommonServices.DataFrameManager import DataFrameManager
from CommonServices.Global_context import GlobalContext

global_context=GlobalContext()

def get():
    
    # return f"MLT^_^ONAIR-PREVIEW-STATE^_^PGM|WEBRTC|{global_context.get_value('pgm')}~PST|WEBRTC|{global_context.get_value('pst')}"
    return f"{global_context.get_value('channel_name')}^_^ONAIR-PREVIEW-STATE^_^PGM|WEBRTC|NULL~PST|WEBRTC|NULL"
 