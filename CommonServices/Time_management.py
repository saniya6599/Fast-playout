import datetime
from CommonServices.Logger import Logger



class timemgt : 
   
 def __init__(self):
   self.logger=Logger()
 
#  def get_system_time_ltc():
#          try:
#             now = datetime.datetime.now()
#             ltc_time = now.strftime("%H:%M:%S:%f")[:-4]  # Trimming microseconds to get frames
#             # self.logger.info(f"System time LTC retrieved: {ltc_time}")
#             # print(f"[INFO] System time LTC retrieved: {ltc_time}")
#             return ltc_time
#          except Exception as e:
#             # self.logger.error(f"Error retrieving system time LTC: {e}")
#             # print(f"[ERROR] Error retrieving system time LTC: {e}")
#             return None
         
#  def get_system_time_ltc():
#     try:
#         now = datetime.datetime.now()
#         return now.strftime("%H:%M:%S:") + f"{int(now.microsecond / 40000):02d}"
#     except Exception:
#         return None

 def get_system_time_ltc(fps=25):
    now = datetime.datetime.now()
    total_microseconds = (now.hour * 3600 + now.minute * 60 + now.second) * 1_000_000 + now.microsecond
    frame_duration_us = int(1_000_000 / fps)
    frame_number = (total_microseconds // frame_duration_us) % fps
    return now.strftime("%H:%M:%S:") + f"{frame_number:02d}"
 
 def get_frame_from_time(time_str, fps=25):

    hh, mm, ss, ff = map(int, time_str.split(':'))
    total_frames = (hh * 3600 + mm * 60 + ss) * fps + ff
    return total_frames

 def get_time_from_frame(total_frames, fps=25):
    
    hh = total_frames // (3600 * fps)
    total_frames %= (3600 * fps)
    mm = total_frames // (60 * fps)
    total_frames %= (60 * fps)
    ss = total_frames // fps
    ff = total_frames % fps
    return f"{hh:02}:{mm:02}:{ss:02}:{ff:02}"

