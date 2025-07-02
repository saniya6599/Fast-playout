import pandas as pd
import json
from CommonServices.DataFrameManager import DataFrameManager
from CommonServices.Global_context import GlobalContext

global_context=GlobalContext()


def fix_play_status():
        return f"{global_context.get_value('channel_name')}^_^FIX-PLAY-STATUS^_^FALSE~~\n"