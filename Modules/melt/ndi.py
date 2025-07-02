import os
import selectors
import subprocess
import sys
import threading
import time
from CommonServices.Global_context import GlobalContext
from CommonServices.DataFrameManager import DataFrameManager


class NDIPlayer:
    def __init__(self):
        self.melted_executable_path = "/home/pmsl/melted/src/melted/src/"
        self.global_context = GlobalContext()
        self.df_manager = DataFrameManager()
        self.update_thread_stop_event = threading.Event()  # Initialize the stop event
        self.on_air_guid = None
        self.next_guid = None


    def play_videos(self):
        # Retrieve the content location
        content_loc = self.global_context.get_value("ContentLoc")
        
        # Get the video file names from the DataFrame
        playlist = self.extract_video_filenames()
        
       # Construct the video files part of the command
        video_files = ' '.join([os.path.join(content_loc, item['filename']) for item in playlist])

        # Construct the full command
        command = f"melt {video_files} -consumer ndi:demo -filter watermark:/home/saniya/videos/cloudx_logo.png"
        
        
         # Start video processing in a separate thread
        process_thread = threading.Thread(target=self.run_command_and_capture_output, args=(command,))
        process_thread.start()

        # Delay starting the update status thread
        time.sleep(5)

        # Run the update_status_periodically method in a separate thread
        update_thread = threading.Thread(target=self.update_status_periodically)
        update_thread.start()

        # Wait for the process to complete
        process_thread.join()

        # Stop the update thread when playback is done
        self.update_thread_stop_event.set()
        update_thread.join()

        # Return the desired string once the video starts
        return "MLT^_^DIRECT-PLAY^_^ACK"      

        # # Run the command in the specified directory and capture output
        # try:
        #     process = subprocess.Popen(
        #         command, 
        #         shell=True, 
        #         cwd=self.melted_executable_path,
        #         stdout=subprocess.PIPE,
        #         stderr=subprocess.PIPE,
        #         text=True
        #     )

        #     # Use selectors for non-blocking IO
        #     sel = selectors.DefaultSelector()
        #     sel.register(process.stdout, selectors.EVENT_READ)
        #     sel.register(process.stderr, selectors.EVENT_READ)

        #     while True:
        #         events = sel.select(timeout=1)
        #         if not events:
        #             # Handle timeout: if no events occur, continue looping or break if needed
        #             continue

        #         for key, _ in events:
        #             output = key.fileobj.readline()
        #             if output:
        #                 # Print and parse the current position from the output
        #                 sys.stdout.write(output)
        #                 sys.stdout.flush()
        #                 if "Current Position" in output:
        #                     current_position = output.split(":")[-1].strip()
        #                     self.global_context.set_value("elapsed_frames", current_position)
        #                     print(f"Elapsed Frames: {current_position}")

        #         # Check if the process is done
        #         if process.poll() is not None:
        #             break
        #     print("Playback finished.")
        # except subprocess.CalledProcessError as e:
        #     print(f"Error during playback: {e}")
        # except Exception as e:
        #     print(f"An unexpected error occurred: {e}")
            
        # Stop the update thread when playback is done
    #     self.update_thread_stop_event.set()
    #     update_thread.join()
            
    #   # Return the desired string once the video starts
    #     return "MLT^_^DIRECT-PLAY^_^ACK"       
           
           
    def run_command_and_capture_output(self, command):
        try:
            process = subprocess.Popen(
                command, 
                shell=True, 
                cwd=self.melted_executable_path,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            sel = selectors.DefaultSelector()
            sel.register(process.stdout, selectors.EVENT_READ)
            sel.register(process.stderr, selectors.EVENT_READ)

            while True:
                events = sel.select(timeout=1)
                if not events:
                    continue

                for key, _ in events:
                    output = key.fileobj.readline()
                    if output:
                        sys.stdout.write(output)
                        sys.stdout.flush()
                        if "Current Position" in output:
                            current_position = output.split(":")[-1].strip()
                            self.global_context.set_value("elapsed_frames", current_position)
                            print(f"Elapsed Frames: {current_position}")

                if process.poll() is not None:
                    break
            print("Playback finished.")
        except subprocess.CalledProcessError as e:
            print(f"Error during playback: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")      
           
           
            
    def extract_video_filenames(self):
       # Access the DataFrame
        df = self.df_manager.get_dataframe()

        # Retrieve the content location
        content_loc = self.global_context.get_value("ContentLoc")

        video_files_with_details = [] 

        # Assume the column with video file names is called 'Inventory'
        video_filenames = df['Inventory'].tolist()

        for filename in video_filenames:
            # Construct the full path to the file
            filepath_without_extension = os.path.join(content_loc, filename)

            # Find the file with its extension
            for root, dirs, files in os.walk(content_loc):
                for file in files:
                    if file.startswith(filename):
                        full_path = os.path.join(root, file)
                        _, extension = os.path.splitext(file)
                        if extension:
                            # Get duration and GUID from DataFrame
                            file_row = df[df['Inventory'] == filename].iloc[0]
                            duration = file_row['Duration']
                            guid = file_row['GUID']

                            # Append filename, extension, duration, and GUID to the list
                            video_files_with_details.append({
                                "filename": filename + extension,
                                "duration": duration,
                                "guid": guid
                            })
                            break

        return video_files_with_details

    def update_status_periodically(self):
        while not self.update_thread_stop_event.is_set():
            # Retrieve the playlist
            playlist = self.extract_video_filenames()
            
            if not playlist:
                print("No video files found in the playlist.")
                time.sleep(1)
                continue

            # Get the current elapsed frames
            elapsed_frames = self.global_context.get_value("elapsed_frames")

            # Check if we are playing and have a valid elapsed frame count
            if int(elapsed_frames) > 0:
                status = "playing"
                self.global_context.set_value("automsg", status)
                self.global_context.set_value("status", status)

                # Set the first video in the playlist as the on-air GUID
                if self.on_air_guid is None:
                    self.on_air_guid = playlist[0]['guid']
                    self.global_context.set_value("onair_guid", self.on_air_guid)

                # Fetch details of the current on-air video
                df = self.df_manager.get_dataframe()
                event_row = df.loc[df['GUID'] == self.on_air_guid]
                if not event_row.empty:
                    # Duration of the current video
                    current_duration_str = event_row.iloc[0]['Duration']
                    current_duration_frames = self.calculate_frames_from_duration(current_duration_str)
                    total_frames=current_duration_frames
                    self.global_context.set_value("total_frames", total_frames)
                    
                      # Get the current video file name
                    filename = event_row.iloc[0]['Inventory']
                    self.global_context.set_value("channel_props", os.path.basename(filename))
                    
                      # Calculate and set the remaining time
                    remaining_time = self.calculate_remaining_time(total_frames, elapsed_frames)
                    self.global_context.set_value("remaining_time", remaining_time)



                    # Check if we have elapsed the duration of the current video
                    if elapsed_frames >= current_duration_frames:
                        # Move to the next video in the playlist
                        current_index = next((i for i, item in enumerate(playlist) if item['guid'] == self.on_air_guid), None)
                        
                        if current_index is not None and current_index < len(playlist) - 1:
                            self.next_guid = playlist[current_index + 1]['guid']
                        else:
                            self.next_guid = None
                        
                        self.global_context.set_value("ready_guid", self.next_guid)

                        # Update the on-air GUID to the next video
                        if self.next_guid:
                            self.on_air_guid = self.next_guid
                            self.global_context.set_value("onair_guid", self.on_air_guid)

                        # Reset elapsed frames for the next video
                        self.global_context.set_value("elapsed_frames", 0)
                        
                        # Optionally update other status values
                        self.global_context.set_value("status", "playing")
                        
                        # Fetch the new event time for the updated on-air GUID
                        event_row = df.loc[df['GUID'] == self.on_air_guid]
                        if not event_row.empty:
                            event_time = event_row.iloc[0]['EventTime']
                            self.global_context.set_value("event_time", event_time)

            # Sleep to prevent tight loop
            time.sleep(1)

    def calculate_elapsed_time(self, elapsed_frames, frame_rate=25):
        elapsed_seconds = elapsed_frames / frame_rate
        hours = int(elapsed_seconds // 3600)
        minutes = int((elapsed_seconds % 3600) // 60)
        seconds = int(elapsed_seconds % 60)
        frames = int((elapsed_seconds - int(elapsed_seconds)) * frame_rate)

        # Format the time as HH:mm:ss:ff
        elapsed_time = f"{hours:02}:{minutes:02}:{seconds:02}:{frames:02}"
        return elapsed_time

    def calculate_remaining_time(self, total_frames, elapsed_frames, frame_rate=25):
        remaining_frames = total_frames - elapsed_frames
        remaining_seconds = remaining_frames / frame_rate
        hours = int(remaining_seconds // 3600)
        minutes = int((remaining_seconds % 3600) // 60)
        seconds = int(remaining_seconds % 60)
        frames = int((remaining_seconds - int(remaining_seconds)) * frame_rate)

        # Format the time as HH:mm:ss:ff
        remaining_time = f"{hours:02}:{minutes:02}:{seconds:02}:{frames:02}"
        return remaining_time