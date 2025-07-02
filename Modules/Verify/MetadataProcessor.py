import os
import json
import subprocess
import time
from CommonServices.config import Config
class MetadataProcessor:
    def __init__(self, json_file, content_dir):
        self.json_file = json_file
        self.content_dir = content_dir

    def get_metadata(file_path):
     try:
        cmd = [
            'ffprobe', '-v', 'error', '-show_entries',
            'format=format_name:stream=codec_name,width,height,duration:stream_tags=creation_time',
            '-of', 'json', file_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        metadata = json.loads(result.stdout)

        # Extracting relevant information
        format_name = metadata['format']['format_name']
        resolution = f"{metadata['streams'][0]['width']}x{metadata['streams'][0]['height']}"
        duration = metadata['format']['duration']
        creation_time = metadata['streams'][0]['tags']['creation_time']
        audio_track = metadata['streams'][0]['codec_name']

        return {
            'format': format_name,
            'resolution': resolution,
            'duration': duration,
            'creation_time': creation_time,
            'audio_track': audio_track
        }
     except subprocess.CalledProcessError as e:
        print(f"Error getting metadata for {file_path}: {e}")
        return None

    def process_videos(content_dir):
     for filename in os.listdir(content_dir):
        if filename.endswith('.mp4') or filename.endswith('.avi'):  # Adjust file extensions as needed
            file_path = os.path.join(content_dir, filename)
            metadata = get_metadata(file_path)
            if metadata:
                # Write metadata to JSON file
                json_file = os.path.splitext(file_path)[0] + '_metadata.json'
                with open(json_file, 'w') as f:
                    json.dump(metadata, f)
                print(f"Metadata written to {json_file}")
            else:
                print(f"Failed to retrieve metadata for {file_path}")

if __name__ == "__main__":
    json_file = "/home/pmsl/videos/content.json"
    content_dir ="/home/pmsl/videos"
    processor = MetadataProcessor(json_file, content_dir)
    processor.process_videos()
