import json
from datetime import datetime, timedelta
import time
from CommonServices.Global_context import GlobalContext
#from Modules.melted.MeltedClient import MeltedClient  # Adjust the import based on your file structure
from CommonServices.Time_management import timemgt



class check_status:
    
    def __init__(self):
      pass
    
    def status(self):

        global_context = GlobalContext()
        
        #  # Wait only if LIVE-SWITCH is in progress
        # if global_context.is_live_running():
        #     print("Waiting for LIVE-SWITCH updates to complete...")
        #     while global_context.is_live_running():
        #         time.sleep(0.1)  # Poll every 100ms    
                
        data_string = (
            '"status":"success",'
            '"time":"",'
            f'"response":"{global_context.get_value("running_id")}|{global_context.get_value("elapsed_frames")}|{global_context.get_value("total_frames")}",'    #ID|frames|frames
            f'"automsg":"{global_context.get_value("automsg")}",'
            f'"readycount":1,'
            f'"elapsed":"{global_context.get_value("elapsed_frames")}",'
            f'"elapsedTime":"{global_context.get_value("elapsed_time")}",'
            f'"systemTime":"{timemgt.get_system_time_ltc()}",'
            f'"remainingTime":"{global_context.get_value("remaining_time")}",'
            f'"sync":"{global_context.get_value("sync")}",'
            f'"inProgress":0,'
            f'"rowscount":{global_context.get_value("rows_count") or 0},'
            f'"firstrow":"{global_context.get_value("first_row_guid")}",'
            f'"lastrow":"{global_context.get_value("last_row_guid")}",'
            f'"onairmsg":"{global_context.get_value("onair_guid")}",'
            f'"readymsg0":"{global_context.get_value("ready_guid")}",'
            f'"eventtime":"{global_context.get_value("event_time")}",'
            f'"paststatus":"{global_context.get_value("onair_guid")}~ON AIR",'
            '"preview":"False",'
            '"subtitle":"False~False~False",'
            '"fixplay":"False~~",'
            f'"channelprops":"{global_context.get_value("channel_props")}",'   
            '"runningfrom":"LOCAL",'
            '"currentstatus":"Verification Process Complete",'
            '"enginepausemode":"False",'
            '"refreshid":"",'
            f'"syncstatus":"IN-ACTIVE | MLT | ENGINE NOT CONNECTED | LAST SYNC TIME : {timemgt.get_system_time_ltc()}",'
            '"checksum":""'
        )

        return(data_string)



    
    # json_response = {
    #     "status": "success",
    #     "time": "",
    #     "response": f"{global_context.get_value('channel_props')}|{global_context.get_value('elapsed_frames')}|{global_context.get_value('total_frames')}",
    #     "automsg": "playing",
    #     "readycount": 1,  
    #     "elapsed": global_context.get_value("elapsed_frames"),
    #     "elapsedTime": global_context.get_value("elapsed_time"),
    #     "systemTime": timemgt.get_system_time_ltc(),
    #     "remainingTime": global_context.get_value("remaining_time"),
    #     "sync": 0,  
    #     "inProgress": global_context.get_value("InProgress"),
    #     "rowscount": global_context.get_value("rows_count"),
    #     "firstrow": global_context.get_value("first_row_guid"),
    #     "lastrow":  global_context.get_value("last_row_guid"),
    #     "onairmsg": global_context.get_value("onair_guid"),
    #     "readymsg0": global_context.get_value("ready_guid"),
    #     "eventtime": global_context.get_value("event_time"),
    #     "paststatus": f"{global_context.get_value('onair_guid')}~ON AIR",
    #     "preview": "False",
    #     "subtitle": "False~False~False",
    #     "fixplay": "False~~",
    #     "channelprops": global_context.get_value("channel_props"),
    #     "runningfrom": "LOCAL",
    #     "currentstatus": "Verification Process Complete",
    #     "enginepausemode": "False",
    #     "refreshid": "",
    #     "syncstatus": f"IN-ACTIVE | MLT | ENGINE NOT CONNECTED | LAST SYNC TIME : {global_context.get_value('ltc_time')}",
    #     "checksum": ""
    # }

    # return json.dumps(json_response)
