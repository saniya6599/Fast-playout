
import threading
import pandas as pd
import numpy as np


def FireTemplate(df, sec_guids, primary_event_time,primary_duration):
    
    import time
    from Modules.Graphics import Fastapi
    from CommonServices.Time_management import timemgt
    from CommonServices.Global_context import GlobalContext
    from app.services.scteservice import ScteService
    from app.services.live_service import live_service
    from CommonServices.Logger import Logger


    global_context=GlobalContext()
    scte_service=ScteService()
    logger=Logger()

    def parse_time_to_frames(time_str):
        try:
            # Assuming time format is HH:mm:ss:ff
            hours, minutes, seconds, frames = map(int, time_str.split(':'))
            total_frames = (hours * 3600 + minutes * 60 + seconds) * 25 + frames
            return total_frames
        except ValueError:
            return 0

    def frames_to_time_format(total_frames):
        hours = total_frames // (3600 * 25) % 24
        minutes = (total_frames % (3600 * 25)) // (60 * 25)
        seconds = (total_frames % (60 * 25)) // 25
        frames = total_frames % 25
        return f"{hours:02}:{minutes:02}:{seconds:02}:{frames:02}"

    
    primary_event_frames = parse_time_to_frames(primary_event_time)

    api = Fastapi.SPXGraphicsAPI(global_context.get_value("gfx_server"))

    try:
        Currentprimary_guid=""
        for guid in sec_guids:
            # Fetch the event time of the secondary GUID
            event_time_str = df.loc[df['GUID'] == guid, 'EventTime'].values[0]
            inventory = df.loc[df['GUID'] == guid, 'Inventory'].values[0]
            duration = df.loc[df['GUID'] == guid, 'Duration'].values[0]
            
            
            # scte marker alert
            if (str(inventory).startswith("SCTEON")):
                if(global_context.get_value('IsScte')):
                    # print(f"SCTE marker detected : {inventory}; sending UDP packet!")
                    threading.Thread(target=scte_service.process_scte_marker, args=(df,primary_event_time,event_time_str,guid,inventory,primary_duration,duration)).start()
                    break
                else : 
                    logger.info(f"Scte is disabled for channel : {global_context.get_value('channel_name')}")
                    break
                    
             
            # elif str(inventory).startswith("LIVE"):
            #  threading.Thread(target=live_service.process_live_udp,args=()).start()

            else:
                def graphics_playout():
                    # current_time = timemgt.get_system_time_ltc()
                    # current_frames = parse_time_to_frames(current_time)
                    # event_time_frames = parse_time_to_frames(event_time_str)
                    # exact_event_frames = primary_event_frames + event_time_frames
                    # delay_frames = exact_event_frames - current_frames
                    # delay_seconds = delay_frames / 25.0
                    
                    #new calculations
                    duration_seconds=parse_time_to_frames(duration)/25.0
                    exact_time_trigger= frames_to_time_format(primary_event_frames + parse_time_to_frames(event_time_str))
                    print(f"At exact {exact_time_trigger}, {inventory} will be triggered! ")
                    Sec_frames = parse_time_to_frames(event_time_str)
                    delay_seconds = Sec_frames / 25.0
                    
                    if delay_seconds > 0:
                        time.sleep(delay_seconds-1)
                    
                    
                    

                    # if delay_seconds > 0:
                    #     exact_event_time_str = frames_to_time_format(exact_event_frames)
                    #     print(f"Waiting for {delay_seconds} seconds until event time {exact_event_time_str} for graphics : {inventory}.")
                    #     time.sleep(delay_seconds)

                    # print(f"Firing template {inventory} at event time {frames_to_time_format(exact_event_frames)}.")
                    print(f"starting a thread to direct playout template for : {inventory}")
                    threading.Thread(target=api.direct_playout,args=(inventory,duration_seconds)).start()
                    # print("Direct Playout Result:", playout_result)



                    # # print(f"Waiting for {duration_seconds} seconds to stop all layers.")
                    # time.sleep(duration_seconds)

                    # #stop_result = api.Panic()
                    # stop_result = api.direct_playout_out(inventory)
                    
                    
                if(global_context.get_value("IsGraphics")):
                 graphics_playout()  
  
                        
            #region old logic    
                        
            # # Calculate the exact event time in frames by adding primary event frames to the secondary event frames
            # exact_event_frames = primary_event_frames + event_time_frames

            # # Calculate the delay in frames
            # delay_frames = exact_event_frames - current_frames

            # # Convert delay frames to seconds (1 frame = 1/25 seconds)
            # delay_seconds = delay_frames / 25.0

            # # If the event time is in the future, wait until it's time to fire the template
            # if delay_seconds > 0:
            #     exact_event_time_str = frames_to_time_format(exact_event_frames)
            #     print(f"Waiting for {delay_seconds} seconds until event time {exact_event_time_str}.")
            #     time.sleep(delay_seconds)

            # print(f"Firing template for secondary GUID {guid} at event time {frames_to_time_format(exact_event_frames)}.")
            # playout_result = api.direct_playout(inventory)
            # print("Direct Playout Result:", playout_result)
            
            #  # Wait for the duration before stopping all layers
            # duration_seconds = duration_frames/25.0
            # print(f"Waiting for {duration_seconds} seconds to stop all layers.")
            # time.sleep(duration_seconds)

            # stop_result = api.Panic()
            # print("Stop All Layers Result:", stop_result)
            

    except Exception as e:
        print(f"An error occurred while firing the template: {str(e)}")



    
    