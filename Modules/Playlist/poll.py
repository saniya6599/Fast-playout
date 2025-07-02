from CommonServices.Global_context import GlobalContext

def poll():
    global_context=GlobalContext()
    
    # return "MLT,192.168.100.18/5050,POLL,17:14:09:10,25,HLSLIVE1~HLSLIVE2,HD,MAIN,750,.mxf\n"

    return f"{global_context.get_value('channel_name')},192.168.100.18/5050,POLL,17:14:09:10,25,,HD,MAIN,750,.mp4\n"
