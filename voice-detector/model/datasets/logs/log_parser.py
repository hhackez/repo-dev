import re
from glob import glob
import numpy as np
from tqdm import tqdm

def process_log_file(log_file_path):
    voice_detected_tracks = []
    voice_not_detected_tracks = []
    try:
        with open(log_file_path, 'r') as file:
            for line in file:
                if "Voice detected" in line:
                    track_info = eval(line.split("Voice detected: ")[1])
                    voice_detected_tracks.append(track_info['track_id'])
                elif "Voice is not detected" in line:
                    track_info = eval(line.split("Voice is not detected: ")[1])
                    voice_not_detected_tracks.append(track_info['track_id'])
        return voice_detected_tracks, voice_not_detected_tracks

    except FileNotFoundError:
        print("Log file not found")


# 사용 예시
track_id_list = []
voice_detected_tracks = []
voice_not_detected_tracks = []
for filename in tqdm(glob('*.log'), desc='iter ...'):
    temp_voice_detected, temp_voice_not_detected = process_log_file(filename)
    voice_detected_tracks.extend(temp_voice_detected)
    voice_not_detected_tracks.extend(temp_voice_not_detected)

print("Voice detected tracks:", len(voice_detected_tracks))
print("Voice not detected tracks:", len(voice_not_detected_tracks))