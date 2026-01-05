import os
from datetime import datetime, timedelta, timezone

# --- 기본 설정 ---
KST = timezone(timedelta(hours=9))
TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')

# --- Trino DB 접속 정보 ---
TRINO_CONN_INFO = {
    "host": "trino.dev.music-flo.io",
    "port": 8889,
    "user": "shen"
}

# --- 파일 및 디렉토리 경로 ---
# 로그 디렉토리가 없으면 생성
if not os.path.exists('logs'):
    os.makedirs('logs')

SRC_FILE = "datasets/lyn_slyn_20250904.txt"
# SRC_FILE = "datasets/input_source_daily.txt"
ERROR_TRACKS_FILE = f"logs/error_tracks_{TIMESTAMP}.txt"
RESULT_CSV_FILE = f"logs/result_data_csv_{TIMESTAMP}.csv"

# --- CSV 헤더 ---
CSV_HEADER = [
    'track_id_src', 'text_length', 'all_seg_cnt', 'total_duration',
    'speech_duration', 'speech_ratio', 'nsp06_seg_cnt',
    'nsp06_word_cnt', 'nsp06_unique_word_cnt'
]

# --- 병렬 처리 설정 ---
NUM_GPUS = 3
NUM_AUDIO_LOADERS = 4
