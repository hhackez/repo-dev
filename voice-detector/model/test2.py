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

# --- 공통 설정 및 로그 파일 ---
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
error_tracks = f"logs/error_tracks_{timestamp}.txt"
filename_csv = f"logs/result_data_csv_{timestamp}.csv"
kst = timezone(timedelta(hours=9))

# --- GPU 워커 프로세스 함수 (이전 아티팩트에서 가져옴, main.py 로직 통합) ---
def whisper_gpu_worker(gpu_id: int, input_queue: multiprocessing.Queue, output_queue: multiprocessing.Queue):
    """
    각 GPU를 시뮬레이션하는 워커 프로세스입니다.
    큐에서 오디오 데이터를 가져와 Whisper 처리를 시뮬레이션하고 결과를 큐에 넣습니다.
    main.py의 음성 감지 및 분석 로직을 포함합니다.
    """
    print(f"GPU {gpu_id} 워커 시작.")
    # 각 워커 프로세스에서 WhisperVoiceDetector 인스턴스를 생성
    voice_detector = WhisperVoiceDetector()

    # 실제 GPU 환경이라면 여기서 torch.cuda.set_device(gpu_id)를 호출하여 GPU 할당
    # 이 부분은 'spawn' 방식으로 시작했을 때 유효합니다.
    try:
        if torch.cuda.is_available():
            torch.cuda.set_device(gpu_id)
            print(f"GPU {gpu_id} 워커: GPU {gpu_id}로 설정 완료.")
        else:
            print(f"GPU {gpu_id} 워커: GPU 사용 불가, CPU로 동작합니다.")
    except Exception as e:
        print(f"GPU {gpu_id} 워커: GPU 설정 중 오류 발생: {e}")


    while True:
        try:
            track_id_src, speech_audio = input_queue.get(timeout=10) # 큐에서 작업 가져오기
            if track_id_src is None: # 종료 시그널
                print(f"GPU {gpu_id} 워커 종료 시그널 수신. ",datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S"))
                break

            print(f"[GPU {gpu_id}] track_id {track_id_src} Whisper 처리 시작... ",datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S"))

            # --- main.py의 음성 감지 및 분석 로직 시작 ---
            result, transcriptions = voice_detector.voice_detect(speech_audio)

            # print(f"track id : {track_id_src}") # 워커 내부 로그
            text_length = len(transcriptions['text'])
            all_seg_cnt = len(transcriptions['segments'])
            # print(f"sgement cnt : {all_seg_cnt}")

            new_text_segments = [
                segment['text']
                for segment in transcriptions['segments']
                if segment.get('no_speech_prob',0) < 0.6
            ]
            nsp06_seg_cnt = len(new_text_segments)
            # print(f"no_speech_prob 0.6 : {nsp06_seg_cnt}")

            new_text = " ".join(new_text_segments)
            tokens = re.findall(r'\S+', new_text.lower())
            nsp06_word_cnt = len(tokens)
            nsp06_word_freq = Counter(tokens)
            nsp06_unique_word_cnt = len(nsp06_word_freq)
            # print(nsp06_unique_word_cnt)

            segments = transcriptions.get("segments", [])
            total_duration = transcriptions['segments'][-1]['end'] if segments else 0
            speech_duration = sum(seg['end'] - seg['start'] for seg in segments if seg.get('no_speech_prob',0) < 0.6)
            speech_ratio = speech_duration / total_duration if total_duration else 0
            # --- main.py의 음성 감지 및 분석 로직 끝 ---

            # 결과 데이터셋 생성 (main.py의 result_data_csv와 동일한 형식)
            processed_data = [
                track_id_src, text_length, all_seg_cnt, total_duration,
                speech_duration, speech_ratio, nsp06_seg_cnt,
                nsp06_word_cnt, nsp06_unique_word_cnt
            ]

            output_queue.put(processed_data)
            print(f"[GPU {gpu_id}] track_id {track_id_src} 처리 완료. 결과 큐에 추가. ",datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S"))
            
            # --- 캐시 해제 로직 추가 ---
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
                print(f"[GPU {gpu_id}] track_id {track_id_src} 처리 후 CUDA 캐시 해제 완료. ",datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S"))

        except queue.Empty:
            # 큐가 비어있으면 잠시 기다렸다가 다시 시도
            time.sleep(1)
        except Exception as e:
            print(f"[GPU {gpu_id}] track_id {track_id_src if 'track_id_src' in locals() else 'Unknown'} 처리 중 오류 발생: {e} ",datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S"))
            # 오류 발생 시에도 결과를 큐에 넣어 메인 프로세스가 대기하지 않도록 함 (오류 정보 포함)
            output_queue.put([track_id_src if 'track_id_src' in locals() else 'Unknown', f"ERROR: {e}"])


# --- 메인 실행 로직 (기존 main.py의 실행 흐름을 병렬 처리로 변경) ---
if __name__ == "__main__":
    # 이 부분에 multiprocessing 시작 방식을 'spawn'으로 설정합니다.
    # 이 코드는 다른 프로세스를 생성하기 전에 호출되어야 합니다.
    try:
        multiprocessing.set_start_method('spawn', force=True)
        print("✅ multiprocessing 시작 방식을 'spawn'으로 설정했습니다.")
    except RuntimeError as e:
        print(f"❌ multiprocessing 시작 방식 설정 오류: {e}. 이미 설정되었거나 다른 문제가 있을 수 있습니다.")


    if torch.cuda.is_available():
        print("✅ GPU 사용 가능:", torch.cuda.get_device_name(0))
    else:
        print("❌ GPU 사용 불가 - CPU로 동작 (워커에서 GPU 할당 시도 시 실패할 수 있음)")

    audio_loader = AudioLoader()
    kst = timezone(timedelta(hours=9))
    start_dtime = datetime.now(kst)
    print("start dtime:", start_dtime.strftime("%Y-%m-%d %H:%M:%S"))
    
    # src_file = "datasets/popul_track_1000.txt"
    # src_file = "datasets/all_test.txt"
    src_file = "datasets/lyric_n_pop02_3.txt"

    # 실제 파일 경로에 맞게 수정
    # with open("datasets/popul_track_1000.txt", "r", encoding="utf-8") as f:
    # `datasets/lyric_all_20000.txt` 파일을 읽는다고 가정합니다.
    # 테스트를 위해 더미 트랙 ID를 사용합니다.
    try:
        # 실제 파일 로드 대신 더미 데이터 사용 (파일이 존재하지 않을 경우)
        with open(src_file, "r", encoding="utf-8") as f:
            track_ids = [int(line.strip()) for line in f if line.strip()]
        # print("\n--- 테스트를 위해 더미 track_ids 생성 ---")
        # track_ids = [101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115] # 15개 트랙 ID
        print(f"처리할 트랙 수: {len(track_ids)}\n")
    except FileNotFoundError:
        print("경고: '{src_file}' 파일을 찾을 수 없습니다. 더미 track_ids를 사용합니다.")
        track_ids = [101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115] # 15개 트랙 ID


    # --- 병렬 처리 설정 ---
    num_gpus = 3 # 사용할 GPU (워커 프로세스) 수 (16core는 병렬 처리 개수와는 직접 관련 없음)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()

    # 워커 프로세스 시작
    processes = []
    print("--- GPU 워커 프로세스 시작 ---")
    for i in range(num_gpus):
        p = multiprocessing.Process(target=whisper_gpu_worker, args=(i, input_queue, output_queue))
        processes.append(p)
        p.start()
    print(f"{num_gpus}개의 GPU 워커 프로세스가 시작되었습니다.\n")

    # 오디오 로드 및 입력 큐에 추가
    print("--- 오디오 로드 및 입력 큐에 데이터 추가 시작 ---")
    total_audios_to_process = 0
    for track_id_src in track_ids:
        speech_audio = audio_loader.load_audio(track_id=track_id_src, trim=True)
        if speech_audio is not None:
            input_queue.put((track_id_src, speech_audio))
            print(f"입력 큐에 track_id {track_id_src} 추가됨. (Waveform Shape: {speech_audio.shape}) ",datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S"))
            total_audios_to_process += 1
        else:
            print(f"Track ID: {track_id_src} 로드 실패, 큐에 추가하지 않습니다. ",datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S"))
    print(f"\n총 {total_audios_to_process}개의 트랙이 큐에 추가되었습니다.\n")
    
    print("\n--- 모든 오디오 처리 완료. 워커 종료 시그널 전송 ---")
    # 워커 프로세스 종료 시그널 전송
    for _ in range(num_gpus):
        input_queue.put((None, None)) # None을 종료 시그널로 사용

    # 결과 수집
    result_data_csv = []
    processed_count = 0
    print("--- 결과 수집 시작 ---")
    while processed_count < total_audios_to_process:
        try:
            # 큐에서 결과 가져오기 (충분한 타임아웃 설정)
            processed_data = output_queue.get(timeout=30)
            result_data_csv.append(processed_data)
            processed_count += 1
            print(f"결과 수집 완료: track_id {processed_data[0]} ",datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S"))
        except queue.Empty:
            print("출력 큐에서 결과를 기다리는 중...")
            time.sleep(2)
        except Exception as e:
            print(f"최종 결과 수집 중 오류 발생: {e} ",datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S"))
            break

    # 모든 워커 프로세스가 종료될 때까지 대기
    for p in processes:
        p.join()

    end_dtime = datetime.now(kst)
    print("end dtime:", end_dtime.strftime("%Y-%m-%d %H:%M:%S"))
    total_duration = end_dtime - start_dtime
    print(f"총 소요 시간: {total_duration}")

    print("\n--- 최종 결과 CSV 파일 작성 ---")
    with open(filename_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(['track_id_src', 'text_length', 'all_seg_cnt', 'total_duration', 'speech_duration', 'speech_ratio', 'nsp06_seg_cnt', 'nsp06_word_cnt', 'nsp06_unique_word_cnt'])
        writer.writerows(result_data_csv)
        f.write(f"\nTotal Processing Time: {total_duration}\n") # CSV 파일 끝에 총 소요 시간 기록
    print(f"결과가 '{filename_csv}'에 저장되었습니다.")
    print("\n프로그램 종료.")
