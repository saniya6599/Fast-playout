from flask import Flask,request
import yaml
from CommonServices.Global_context import GlobalContext
from Modules.melted.MeltedClient import MeltedClient
from CommonServices.Logger import Logger


class CustomDumper(yaml.Dumper):
    def __init__(self):
        self.global_context = GlobalContext()
        self.melted_executable_path = self.global_context.get_value("melted_executable_path")
        self.logger=Logger()
        
    
    def increase_indent(self, flow=False, indentless=False):
        return super(CustomDumper, self).increase_indent(flow, False)

    def Create(self,data):
        
        # Add the three new items
        data["pcr_period"] = 20
        data["pix_fmt"] = "yuv420p"
        data["deinterlacer"] = "yadif"
        
        
        yaml_str=yaml.dump(data,default_flow_style=False, sort_keys=False)
        # yaml_str2=yaml_str.replace('"','')
        yml_lines=yaml_str.splitlines()
        # print(yml_lines)
        for i in range(len(yml_lines)):
            if i == 0:
                yaml_str= "- "+ yml_lines[i]
                # print(yaml_str)
            else:
                if "eventname" in yml_lines[i]:
                    pass
                else:
                    yaml_str+='\n  ' +yml_lines[i]
                    # print(yaml_str)

        yaml_str=yaml_str.replace("'","")
        yaml_str+='\n  '

        mode = self.global_context.get_value("OutMode")
        yml_path = f"{self.melted_executable_path}/{self.global_context.get_value('channel_name')}_udp_hdp.yml" if mode == 0 else f"{self.melted_executable_path}/ndi.yml" if mode == 1 else f"{self.melted_executable_path}/decklink.yml" if mode == 2 else None
        if self.melted_executable_path == None:
            print("[Error] Playback server executable path can not be none, Ensure consig.json includes valid settings")
            self.logger.error("Playback server executable path can not be none, Ensure consig.json includes valid settings")
            return False
        else:
            self.global_context.set_value("yml_path",yml_path)
            with open(yml_path, 'w') as outfile:
                outfile.write(yaml_str)
            print(f"[INFO] YAML Configuration Created successfully with mode {mode} at {yml_path}")
            self.logger.info(f"YAML Configuration Created successfully with mode {mode} at {yml_path}")
            # return True
        
        
        
        # Create udp_hdp.conf
            conf_path = f"{self.melted_executable_path}/{self.global_context.get_value('channel_name')}_udp_hdp.conf"
            conf_content = f"""
SET root={self.global_context.get_value('ContentLoc')}
UADD multi:{yml_path}
USET U0 consumer.mlt_profile=atsc_1080p_25
USET U0 consumer.real_time=1
USET U0 consumer.channels=2
USET U0 producer.audio_index=all
USET U0 producer.timecode=10:00:00:00
USET U0 producer.timecode_from_metadata=1
USET U0 filter0.mlt_service=timecode
USET U0 filter0.show=1
""".strip()
            
            try:
                with open(conf_path, 'w') as conf_file:
                    conf_file.write(conf_content)
                print(f"[INFO] UDP Configuration Created successfully at {conf_path}")
                self.logger.info(f"UDP Configuration Created successfully at {conf_path}")
            except Exception as e:
                print(f"[ERROR] Failed to create udp_hdp.conf: {str(e)}")
                self.logger.error(f"Failed to create udp_hdp.conf: {str(e)}")
                return False

            return True