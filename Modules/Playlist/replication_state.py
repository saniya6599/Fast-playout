from CommonServices.Global_context import GlobalContext

def replication_state():
    global_context=GlobalContext()
    return f"{global_context.get_value('channel_name')}^_^REPLICATION-STATE^_^NACK\n"
