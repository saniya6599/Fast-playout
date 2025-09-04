from threading import Thread
import threading
import time
from CommonServices.tcp_connection import TCPConnection
from CommonServices.config import Config
from CommonServices.Global_context import GlobalContext
from CommonServices.APIListener import APIListener
from Modules.Verify.FileWatcher import FileWatcher
from Modules.Verify.MetadataProcessor import MetadataProcessor
from Modules.melted import MeltedClient
from Modules.Graphics import Fastapi
from CommonServices.Logger import Logger
from app.services.live_service import live_service
from Modules.Playlist.check_status import check_status
from CommonServices.AsrunGenerator import AsRunFormatter
from Modules.Playlist.check_status import check_status
from Modules.Playlist import fetch_list
from CommonServices.auto_recovery import AutoRecovery


def start_file_watcher(directory_to_watch, json_file):
    watcher = FileWatcher(directory_to_watch, json_file)
    watcher.run()

def start_metadata_processor(json_file, content_dir):
    processor = MetadataProcessor(json_file, content_dir)
    processor.process_videos()  

def main():
   
    global_context = GlobalContext()
    logger = Logger()
    service = live_service()
    check = check_status()    
    recovery = AutoRecovery()
    config = Config()
    
    logger.log_operation_start("application_startup")
    config.load_config("config.json")     

    global_context.set_value('melted_executable_path', config.get('melted_executable_path'))
    global_context.set_value('melted_port', config.get('melted_port'))
    global_context.set_value('listener_port', config.get('listener_port'))
    global_context.set_value('normalise_som', config.get('normalise_som'))
    
    logger.info("Configuration loaded successfully", 
                melted_path=config.get('melted_executable_path'),
                melted_port=config.get('melted_port'),
                listener_port=config.get('listener_port'))
    
    # global_context.set_value('udp_packet_ip',config.get('Udp_packet').split(':')[0])    #now need to take it from external command (config manager)
    # global_context.set_value('udp_packet_port',config.get('Udp_packet').split(':')[1])
    
    #global_context.set_value('Gfx_Server',config.get("gfx_server"))
   
    # global_context.set_value('scte_address',config.get('scte'))   #now need to take it from external command (config manager)
    # global_context.set_value('cloudx_udp', config.get("cloudx_udp"))
    # global_context.set_value('pgm',config.get("pgm"))
    # global_context.set_value('pst',config.get("pst"))
    
    #for testig purpose:
    # global_context.set_value("switcher",config.get('switcher'))
    # global_context.set_value("switcher_dest",config.get('switcher_dest'))
    

    from Modules.melted.MeltedClient import MeltedClient

    logger.log_operation_start("api_server_startup")
    api_listener = APIListener()
    api_thread = Thread(target=api_listener.start_api_server, daemon=True) 
    api_thread.start()
    logger.log_operation_end("api_server_startup", success=True)
    
     # Initialize and configure AutoRecovery
    # auto_recovery = AutoRecovery(global_context, logger)
    # atexit.register(auto_recovery.perform_recovery)
    
    
    #replace it with webhooks to avoid while loop
    while not global_context.get_value("StartMelted") :
        time.sleep(1)

    
    logger.log_operation_start("playback_server_startup")
    logger.info("Received configuration successfully. Starting server & TCP connection...")
    client = MeltedClient()
    client.start_melted_server()
    # client.start_melted_with_tmux()
    
    tcp_connection = TCPConnection(global_context.get_value("host"), global_context.get_value("port"))
    tcp_connection.start_server()
    logger.log_operation_end("playback_server_startup", success=True)
    
    
    # if global_context.get_value('Isauto_recovery') and global_context.get_value("startfrom"):
    #     if not client.start(global_context.get_value("startfrom")) : 
    #         logger.error("Unable to start playback after recovery, try manually!")
    
    while True:
      try:
        conn = tcp_connection.accept_connection()   
        if conn is None:
            logger.warning("Failed to establish connection, retrying...")
            continue  # Continue to the next iteration of the loop if connection failed

        command = tcp_connection.receive_message(conn)
        
        # Log command received with proper filtering
        logger.log_command_received(command, source="tcp")

        if command is None:
            logger.warning("Received empty command or connection closed, continuing...")
            tcp_connection.close_connection(conn)
            continue  

        global_context.set_value('ChannelName', command.split('^_^')[0])
        
        
        if "DELETE-APPEND-PLAYLIST" in command:
            from Modules.Playlist import delete_appended_playlist
            
            result = delete_appended_playlist.delete(command,client)  
            global_context.get_value('current_playlists').remove(command.split('^_^')[2])
            tcp_connection.send_message(conn, result)
        
        elif "APPEND-PLAYLIST" in command : 
                
            ToAppendplaylist = command.split("^_^")[2]
            ToAppenddate = command.split("^_^")[3]
            result = client.append_playlist(ToAppendplaylist,ToAppenddate)
            if "NACK" in result :
                 tcp_connection.send_message(conn,result)
            else :
                global_context.set_value('channel_props',ToAppendplaylist )
                global_context.get_value('current_playlists').append(ToAppendplaylist)

                for line in result:
                 tcp_connection.send_message(conn, f"\n{line}\n")
                tcp_connection.send_message(conn, "\nCPLISTSTOP\n")
                tcp_connection.send_message(conn, f"{global_context.get_value('channel_name')}^_^APPEND-PLAYLIST^_^{ToAppendplaylist}^_^{ToAppenddate}^_^DONE")
            
        elif "POLL" in command:
            from Modules.Playlist import poll

            result = poll.poll()
            tcp_connection.send_message(conn, result)
            # if(global_context.get_value("ChannelName") is not global_context.get_value("channel_name") ):
            #     print("[ERROR] Channel Name mismatch, Exiting server and restart with correct channel name!")
            #     sys.exit(0)
                
        elif "REPLICATION-STATE" in command:
            from Modules.Playlist import replication_state

            result = replication_state.replication_state()
            tcp_connection.send_message(conn, result)

        elif "SUBTITLE-STATE" in command:
            from Modules.Playlist import subtitle_state

            result = subtitle_state.subtitle_state()
            tcp_connection.send_message(conn, result)
 
        elif "CHECK-STATUS" in command:

                result = check.status()
                tcp_connection.send_message(conn, str(result))
                global_context.set_value("sync",0)

        elif "FETCH-LIST" in command:

            result = fetch_list.fetch_list()

            if not result :
                 tcp_connection.send_message(conn,f"{global_context.get_value('channel_name')}^_^FETCH-LIST^_^NOTHNG-TO-SYNC")
                 global_context.set_value('automsg','automation_stop_linkedlist_empty')
            else :
                
             for line in result:
              tcp_connection.send_message(conn, f"\n{line}\n")
             tcp_connection.send_message(conn, "\nCPLISTSTOP\n")
             if not (global_context.get_value('automsg').startswith('playing')):
              global_context.set_value('automsg','automation_stop')
              
        elif "FETCH-CONTENT" in command:
            from Modules.Playlist import fetch_content
            result = fetch_content.fetch_content(command)

            if not result :
                 tcp_connection.send_message(conn,f"{global_context.get_value('channel_name')}^_^FETCH-CONTENT^_^NACK")
            else :
                
             for line in result:
              tcp_connection.send_message(conn, f"\n{line}\n")
             tcp_connection.send_message(conn, "\nCPLISTSTOP\n")

        elif "GET-PLAYLIST" in command:
            from Modules.Playlist import Get_playlist       

            result = Get_playlist.get_playlist(global_context.get_value("PlaylistLoc"),global_context.get_value("channel_name"))
            tcp_connection.send_message(conn, result)

        elif "LOAD-PLAYLIST" in command:
            from Modules.Playlist import load_playlist
            #from Modules.Verify.PlaylistVerifier import PlaylistVerifier

            global_context.set_value('InProgress',1)  
            FileToLoad = command.split('^_^')[-2]
            result = load_playlist.load_playlist(global_context.get_value("PlaylistLoc"),FileToLoad,global_context.get_value("channel_name"))
            # verifier = PlaylistVerifier()
            # result =verifier.full_playlist_verification()
            global_context.set_value("Playlist_loaded",FileToLoad)
            global_context.set_value('channel_props',FileToLoad)
            global_context.set_value('current_playlists', [FileToLoad])
           
           
            for line in result:
             tcp_connection.send_message(conn, f"\n{line}\n")
            tcp_connection.send_message(conn, "\nCPLISTSTOP\n")
            tcp_connection.send_message(conn, f"{global_context.get_value('channel_name')}^_^LOAD-PLAYLIST^_^DONE")
          
        elif "SAVE-PART-PLAYLIST" in command:
            from Modules.Playlist import save_playlist
            
            result = save_playlist.save_part_playlist(command)  
            tcp_connection.send_message(conn, result)
            
        elif "SAVE-FULL-PLAYLIST" in command:
            from Modules.Playlist import save_playlist
            
            result = save_playlist.save_full_playlist(command)  
            tcp_connection.send_message(conn, result)
                     
        elif "DELETE-PLAYLIST" in command:
            from Modules.Playlist import delete_playlist
            
            result = delete_playlist.delete(command)  
            tcp_connection.send_message(conn, result)
                       
        elif "CUT-PASTE" in command:

            result = client.cut_paste(command)  
            tcp_connection.send_message(conn, result)

        elif "DIRECT-PLAY" in command:
             from Modules.melted.MeltedClient import MeltedClient

             global_context.set_value("GUID",command.split("^_^")[2])
             result = client.start(global_context.get_value("GUID"))
             
             start_asrun_thread()
             tcp_connection.send_message(conn, result)  
             
        # elif "INSTANT-START" in command:
        #      from CommonServices.instant_restart import instant_restart
            
        #      _restart = instant_restart()
        #      result = _restart.start()
        #      tcp_connection.send_message(conn, result)  
          
        
        elif "CONTENT-INSERT" in command:

                result = client.content_insert_entry(command)
                tcp_connection.send_message(conn, result)   
                     
        elif "INSERT" in command:

                result = client.insert_entry(command)
                tcp_connection.send_message(conn, result)   
                           
        elif "BULK-DELETE" in command:

                    guids_str = command.split('^_^')[2]
                    result = client.bulk_delete_entries_by_guids(guids_str)
                    tcp_connection.send_message(conn, result)

        elif "DELETE" in command:   

                    guids_str = command.split('^_^')[2]
                    result = client.delete_entries_by_guids(guids_str)
                    tcp_connection.send_message(conn, result)

        elif "UPDATE" in command:
            
                    result = client.update(command)     
                    tcp_connection.send_message(conn, result)

        elif "PASTE" in command:
                result = client.paste_entries(command) 
                tcp_connection.send_message(conn, result)

        elif "SKIP" in command: 
            
                    result = client.skip_to_next()
                    tcp_connection.send_message(conn, result)
                    
        elif "STOP" in command:

                    result=client.stop_melted()
                    status="automation_stop"
                    global_context.set_value('automsg',"automation_stop")
                    
                    tcp_connection.send_message(conn, f"{global_context.get_value('channel_name')}^_^STOP^_^ACK")
                    
        elif "PAUSE" in command:

                    result=client.pause()
                    tcp_connection.send_message(conn, result)   
                    
        #Physical verification
        elif "VERIFY-SERVER" in command:
            
                    if(global_context.get_value("verification_mode").startswith('Physical')):
                        from Modules.Verify.PlaylistVerifier import PlaylistVerifier

                        verifier = PlaylistVerifier()
                        result =verifier.full_playlist_verification()
                        for line in result:
                            tcp_connection.send_message(conn, f"\n{line}\n")
                        tcp_connection.send_message(conn, "\nCPLISTSTOP\n")
                        tcp_connection.send_message(conn, f"{global_context.get_value('channel_name')}^_^VERIFY-SERVER^_^DONE")
                    else:
                        tcp_connection.send_message(conn, f"{global_context.get_value('channel_name')}^_^VERIFY-SERVER^_^NACK")
                   
        #Database verification            
        elif "VERIFY-ALL" in command:            
                    if not (global_context.get_value("verification_mode").startswith('Physical')):
                        from Modules.Verify.Database_verification import Database_verification

                        verifier = Database_verification()
                        result =verifier.verify()
                        for line in result:
                            tcp_connection.send_message(conn, f"\n{line}\n")
                        tcp_connection.send_message(conn, "\nCPLISTSTOP\n")
                        tcp_connection.send_message(conn, f"{global_context.get_value('channel_name')}^_^VERIFY-SERVER^_^DONE")
                    else:
                        tcp_connection.send_message(conn, f"{global_context.get_value('channel_name')}^_^VERIFY-ALL^_^NACK")           
        
        #secondary-verification
        elif "VERIFY-SECONDARY" in command:
            
                    if not (global_context.get_value("verification_mode").startswith('Physical')):
                        from Modules.Verify.secondary_verification import secondary_verification

                        verifier = secondary_verification()          
                        result =verifier.verify()
                        for line in result:
                            tcp_connection.send_message(conn, f"\n{line}\n")
                        
                        tcp_connection.send_message(conn, "\nCPLISTSTOP\n")
                        tcp_connection.send_message(conn, f"{global_context.get_value('channel_name')}^_^VERIFY-SECONDARY^_^DONE")
                    else:
                        tcp_connection.send_message(conn, f"{global_context.get_value('channel_name')}^_^VERIFY-ALL^_^NACK")

        #Single ID Verification
        elif "VERIFY" in command:
            
                    result = client.verify_entry(command)
                    tcp_connection.send_message(conn, result)
                    
        elif "LOAD-UNLOAD-LOGO" in command: 

                    result = client.Load_unload_logo()
                    tcp_connection.send_message(conn, result)        
                     
        elif "BACKTOBACK-LIVE-SWITCH" in command:
            
                    service =live_service()
                    global_context.set_value('IsLiveRunning',True)
                    result = client.Jump_to_live()
 
                    tcp_connection.send_message(conn, result)   
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
        elif "LIVE-SWITCH" in command:

                    global_context.set_value('IsLiveRunning',True)
                    global_context.set_live_running(True)  # Indicate LIVE-SWITCH is in progress
                    
                    client.stop_update_thread()
                    
                    result = client.Jump_to_live()
                    tcp_connection.send_message(conn, result)
                    global_context.set_live_running(False)  # Reset flag after completion
                                       
        elif "PLAY" in command:
            
                    result = client.switch_to_server()
                    tcp_connection.send_message(conn, result)
                                             
        elif "GET-LIVE-PARAMETER" in command:
            
                    service =live_service()
                    result = service.get_live_sources()
                    tcp_connection.send_message(conn, result)
                    
        elif "ONAIR-PREVIEW-STATE" in command:
                    from Modules.Playlist import onair_preview_state
                    
                    result = onair_preview_state.get()
                    tcp_connection.send_message(conn, result)
                  
        # else:
            # tcp_connection.send_message(conn, "Invalid command.")

        tcp_connection.close_connection(conn)
         
      except KeyboardInterrupt:
          logger.log_operation_start("application_shutdown")
          client.kill_melted_forcefully()   
          if(global_context.get_value('Isauto_recovery')):
            logger.info("Application interrupted. Performing cleanup and recovery!")
            recovery.perform_recovery()
            logger.log_operation_end("application_shutdown", success=True)
            break
          else:
              logger.info("Shutting down without recovery")
              logger.log_operation_end("application_shutdown", success=True)
      except Exception as e:
            logger.log_exception(e, context={'operation': 'main_loop'})
            # print("Application interrupted. Performing cleanup and recovery...")
            # recovery.perform_recovery()
            # break

def start_asrun_thread():
    formatter=AsRunFormatter()
    asrun_thread= threading.Thread(target=formatter.run_asrun_formatter_periodically)
    asrun_thread.daemon = True 
    asrun_thread.start()

if __name__ == "__main__":
        main()
 
