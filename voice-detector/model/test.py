from model.src.audio_loader import AudioLoader
from detectors.whisper_voice_detector import WhisperVoiceDetector
import io
import torchaudio
import requests
import torch
import time
from datetime import datetime, timedelta, timezone
import multiprocessing
import queue
from collections import Counter
import json
import re
import csv
import threading

# --- 공통 설정 및 로그 파일 ---
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
error_tracks = f"logs/error_tracks_{timestamp}.txt"
filename_csv = f"logs/result_data_csv_{timestamp}.csv"
kst = timezone(timedelta(hours=9))

# --- 오디오 로더 워커 함수 ---
def audio_loader_worker(track_id_queue: multiprocessing.Queue, audio_data_queue: multiprocessing.Queue):
    """
    트랙 ID를 받아 오디오를 로드하고, 로드된 데이터를 오디오 데이터 큐에 넣는 워커 프로세스.
    """
    print(f"오디오 로더 워커 {multiprocessing.current_process().pid} 시작.")
    audio_loader = AudioLoader()
    while True:
        track_id = track_id_queue.get()
        if track_id is None: # 종료 시그널
            break
        
        print(f"오디오 로더 {multiprocessing.current_process().pid}: track_id {track_id} 로드 시작.")
        speech_audio = audio_loader.load_audio(track_id=track_id, trim=True)
        if speech_audio is not None:
            audio_data_queue.put((track_id, speech_audio))
            print(f"오디오 로더 {multiprocessing.current_process().pid}: track_id {track_id} 로드 완료, 큐에 추가.")
        else:
            print(f"오디오 로더 {multiprocessing.current_process().pid}: track_id {track_id} 로드 실패.")
            try:
                with open(error_tracks, "a") as f:
                    f.write(f"{track_id}\n")
            except Exception as e:
                print(f"오디오 로더 {multiprocessing.current_process().pid}: 에러 파일 작성 실패 - {e}")

    print(f"오디오 로더 워커 {multiprocessing.current_process().pid} 종료.")


def whisper_gpu_worker(gpu_id: int, audio_data_queue: multiprocessing.Queue, result_queue: multiprocessing.Queue):
    """
    오디오 데이터 큐에서 데이터를 가져와 Whisper 처리를 하고, 결과를 결과 큐에 넣는 워커 프로세스.
    각 워커는 지정된 GPU를 독립적으로 사용합니다.
    """
    device = f"cuda:{gpu_id}"
    print(f"GPU {gpu_id} 워커 시작. 장치: {device}")

    try:
        voice_detector = WhisperVoiceDetector(device=device)
    except Exception as e:
        print(f"GPU {gpu_id} 워커: 모델 로드 중 오류 발생: {e}")
        return

    while True:
        track_id_src = 'Unknown' # 오류 발생 시를 대비해 초기화
        try:
            item = audio_data_queue.get(timeout=20)
            if item is None or item[0] is None: # 종료 시그널
                print(f"GPU {gpu_id} 워커 종료 시그널 수신. ",datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S"))
                break

            track_id_src, speech_audio = item
            print(f"[GPU {gpu_id}] track_id {track_id_src} Whisper 처리 시작... ",datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S"))

            result, transcriptions = voice_detector.voice_detect(speech_audio)
            text_length = len(transcriptions['text'])
            all_seg_cnt = len(transcriptions['segments'])
            new_text_segments = [seg['text'] for seg in transcriptions['segments'] if seg.get('no_speech_prob',0) < 0.6]
            nsp06_seg_cnt = len(new_text_segments)
            new_text = " ".join(new_text_segments)
            tokens = re.findall(r'\S+', new_text.lower())
            nsp06_word_cnt = len(tokens)
            nsp06_unique_word_cnt = len(Counter(tokens))
            segments = transcriptions.get("segments", [])
            total_duration = segments[-1]['end'] if segments else 0
            speech_duration = sum(seg['end'] - seg['start'] for seg in segments if seg.get('no_speech_prob',0) < 0.6)
            speech_ratio = speech_duration / total_duration if total_duration else 0
            
            processed_data = [
                track_id_src, text_length, all_seg_cnt, total_duration,
                speech_duration, speech_ratio, nsp06_seg_cnt,
                nsp06_word_cnt, nsp06_unique_word_cnt
            ]

            result_queue.put(processed_data)
            print(f"[GPU {gpu_id}] track_id {track_id_src} 처리 완료. 결과 큐에 추가. ",datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S"))
            
            if torch.cuda.is_available():
                torch.cuda.empty_cache()

        except queue.Empty:
            print(f"GPU {gpu_id}: 오디오 데이터 큐가 비어있습니다. 대기 중...")
            time.sleep(1)
        except Exception as e:
            # [수정] 오류 발생 시, 에러 파일에만 기록하고 결과 큐에는 넣지 않음
            print(f"[GPU {gpu_id}] track_id {track_id_src} 처리 중 오류 발생: {e} ",datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S"))
            if track_id_src != 'Unknown':
                try:
                    with open(error_tracks, "a") as f:
                        f.write(f"{track_id_src}\n")
                except Exception as file_e:
                    print(f"GPU {gpu_id}: 에러 파일 작성 실패 - {file_e}")
    print(f"GPU {gpu_id} 워커 종료.")


# --- 메인 실행 로직 ---
if __name__ == "__main__":
    try:
        multiprocessing.set_start_method('spawn', force=True)
        print("✅ multiprocessing 시작 방식을 'spawn'으로 설정했습니다.")
    except RuntimeError as e:
        print(f"❌ multiprocessing 시작 방식 설정 오류: {e}.")

    kst = timezone(timedelta(hours=9))
    start_dtime = datetime.now(kst)
    print("start dtime:", start_dtime.strftime("%Y-%m-%d %H:%M:%S"))
    
    # src_file = "datasets/lyric_n_pop02_3.txt"
    src_file = "datasets/lyric_n_10000.txt"
    
    try:
        with open(src_file, "r", encoding="utf-8") as f:
            track_ids = [int(line.strip()) for line in f if line.strip()]
        print(f"처리할 트랙 수: {len(track_ids)}\n")
    except FileNotFoundError:
        print(f"경고: '{src_file}' 파일을 찾을 수 없습니다. 테스트용 더미 track_ids를 사용합니다.")
        track_ids = list(range(101, 121)) # 20개 트랙 ID

    # --- 병렬 처리 설정 ---
    num_gpus = 3 
    num_audio_loaders = 4 # 오디오 로더 프로세스 수
    
    track_id_queue = multiprocessing.Queue()
    audio_data_queue = multiprocessing.Queue(maxsize=num_gpus * 2)
    result_queue = multiprocessing.Queue()

    # --- 워커 프로세스 시작 ---
    loader_processes = []
    print("--- 오디오 로더 워커 프로세스 시작 ---")
    for _ in range(num_audio_loaders):
        p = multiprocessing.Process(target=audio_loader_worker, args=(track_id_queue, audio_data_queue))
        loader_processes.append(p)
        p.start()
    
    whisper_processes = []
    print("--- GPU 워커 프로세스 시작 ---")
    for i in range(num_gpus):
        p = multiprocessing.Process(target=whisper_gpu_worker, args=(i, audio_data_queue, result_queue))
        whisper_processes.append(p)
        p.start()

    # --- CSV 파일 생성 및 헤더 작성 ---
    print("--- 결과 CSV 파일 초기화 ---")
    try:
        with open(filename_csv, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, delimiter=',')
            writer.writerow(['track_id_src', 'text_length', 'all_seg_cnt', 'total_duration', 'speech_duration', 'speech_ratio', 'nsp06_seg_cnt', 'nsp06_word_cnt', 'nsp06_unique_word_cnt'])
        print(f"결과 파일 '{filename_csv}'에 헤더를 작성했습니다.\n")
    except IOError as e:
        print(f"CSV 파일 생성 오류: {e}")
        exit()

    # --- 트랙 ID를 큐에 추가 ---
    print("--- 트랙 ID를 큐에 추가 시작 ---")
    total_tasks = len(track_ids)
    for tid in track_ids:
        track_id_queue.put(tid)
    print(f"총 {total_tasks}개의 트랙 ID를 큐에 추가했습니다.\n")

    # --- [수정] 워커 종료 및 결과 수집 로직 개선 ---

    # 1. 오디오 로더들에게 종료 신호를 보내고, 모든 로더가 작업을 마칠 때까지 대기
    print("--- 모든 트랙 ID 전송 완료. 오디오 로더 종료 대기 ---")
    for _ in range(num_audio_loaders):
        track_id_queue.put(None)
    for p in loader_processes:
        p.join()
    print("--- 모든 오디오 로더 워커가 종료되었습니다. ---")

    # 2. 로더가 모두 종료되었으므로, Whisper 워커들에게 종료 신호를 전송
    print("--- Whisper 워커에게 종료 신호를 보냅니다. ---")
    for _ in range(num_gpus):
        audio_data_queue.put((None, None))

    # 3. 모든 Whisper 워커가 종료되거나, 결과 큐가 완전히 빌 때까지 결과 수집
    processed_count = 0
    print("--- 최종 결과 수집을 시작합니다. ---")
    while any(p.is_alive() for p in whisper_processes) or not result_queue.empty():
        try:
            processed_data = result_queue.get(timeout=5)
            
            with open(filename_csv, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f, delimiter=',')
                writer.writerow(processed_data)
            
            processed_count += 1
            print(f"결과 기록 완료: track_id {processed_data[0]} (총 {processed_count}개 처리)")

        except queue.Empty:
            if not any(p.is_alive() for p in whisper_processes):
                break # 워커가 모두 종료되고 큐가 비었으면 루프 탈출
            else:
                print("결과 큐는 비어있지만, Whisper 워커가 아직 실행 중입니다. 대기합니다...")
                continue
    
    print(f"\n총 {processed_count}개의 트랙이 성공적으로 처리되었습니다.")

    # 4. 모든 Whisper 워커 프로세스가 완전히 종료될 때까지 대기
    print("--- 모든 Whisper 워커 프로세스 종료 대기 ---")
    for p in whisper_processes:
        p.join()
    
    end_dtime = datetime.now(kst)
    print("end dtime:", end_dtime.strftime("%Y-%m-%d %H:%M:%S"))
    total_duration = end_dtime - start_dtime
    print(f"총 소요 시간: {total_duration}")

    with open(filename_csv, 'a', encoding='utf-8', newline='') as f:
        f.write(f"\n# Total Processing Time: {total_duration}\n")
    print(f"모든 결과가 '{filename_csv}'에 저장되었습니다.")
    print("\n프로그램 종료.")
