from CommonServices.Global_context import GlobalContext
from Modules.melted import MeltedClient

def stop():
    global_context=GlobalContext()
    melted = MeltedClient.MeltedClient()
    melted.stop_melted()
    response = f"{global_context.get_value('channel_name')}^_^STOP^_^ACK"
    return response
