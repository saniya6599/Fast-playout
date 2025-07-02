from CommonServices.Global_context import GlobalContext
global_context=GlobalContext()

def subtitle_state():
        return f"{global_context.get_value('channel_name')}^_^SUBTITLE-STATE^_^NACK\n"