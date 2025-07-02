from CommonServices.Global_context import GlobalContext
from Modules.melted import MeltedClient

def skip():
    global_context=GlobalContext()
    melted = MeltedClient.MeltedClient()
    melted.skip_to_next()
    response = f"{global_context.get_value('channel_name')}^_^SKIP^_^ACK"
    return response
