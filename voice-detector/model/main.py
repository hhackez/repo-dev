import multiprocessing
import queue
import time
import csv
from datetime import datetime, timedelta

import config
from workers import audio_loader_worker, whisper_gpu_worker
from create_source import SourceCreator # SourceCreator 클래스 import

class PipelineManager:
    """
    오디오 처리 파이프라인의 설정, 실행, 종료를 관리하는 클래스.
    """
    def __init__(self):
        self.track_ids = self._load_track_ids()
        self.total_tasks = len(self.track_ids)
        self.track_id_queue = multiprocessing.Queue()
        self.audio_data_queue = multiprocessing.Queue(maxsize=config.NUM_GPUS * 2)
        self.result_queue = multiprocessing.Queue()
        self.loader_processes = []
        self.whisper_processes = []

    def _load_track_ids(self):
        """소스 파일에서 트랙 ID를 로드합니다."""
        try:
            with open(config.SRC_FILE, "r", encoding="utf-8") as f:
                ids = [int(line.strip()) for line in f if line.strip()]
            print(f"처리할 트랙 수: {len(ids)}\n")
            return ids
        except FileNotFoundError:
            print(f"경고: '{config.SRC_FILE}' 파일을 찾을 수 없습니다. 테스트용 더미 track_ids를 사용합니다.")
            return list(range(101, 121))

    def _setup_csv_file(self):
        """결과를 저장할 CSV 파일을 생성하고 헤더를 작성합니다."""
        try:
            with open(config.RESULT_CSV_FILE, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(config.CSV_HEADER)
            print(f"결과 파일 '{config.RESULT_CSV_FILE}'에 헤더를 작성했습니다.\n")
        except IOError as e:
            print(f"CSV 파일 생성 오류: {e}")
            exit()

    def _start_workers(self):
        """오디오 로더 및 Whisper 워커 프로세스를 시작합니다."""
        print("--- 오디오 로더 워커 프로세스 시작 ---")
        for _ in range(config.NUM_AUDIO_LOADERS):
            p = multiprocessing.Process(target=audio_loader_worker, args=(self.track_id_queue, self.audio_data_queue))
            self.loader_processes.append(p)
            p.start()
        
        print("--- GPU 워커 프로세스 시작 ---")
        for i in range(config.NUM_GPUS):
            p = multiprocessing.Process(target=whisper_gpu_worker, args=(i, self.audio_data_queue, self.result_queue))
            self.whisper_processes.append(p)
            p.start()

    def _process_results(self):
        """결과 큐에서 데이터를 가져와 파일에 기록합니다."""
        processed_count = 0
        print("--- 최종 결과 수집을 시작합니다. ---")
        while any(p.is_alive() for p in self.whisper_processes) or not self.result_queue.empty():
            try:
                processed_data = self.result_queue.get(timeout=5)
                with open(config.RESULT_CSV_FILE, 'a', encoding='utf-8', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(processed_data)
                
                processed_count += 1
                print(f"결과 기록 완료: track_id {processed_data[0]} (총 {processed_count}개 성공)")
            except queue.Empty:
                if not any(p.is_alive() for p in self.whisper_processes):
                    print("모든 Whisper 워커가 종료되었고 결과 큐가 비어있으므로 수집을 중단합니다.")
                    break
                else:
                    print("결과 큐는 비어있지만, Whisper 워커가 아직 실행 중입니다...")
        
        print(f"\n총 {processed_count}개의 트랙이 성공적으로 처리되었습니다.")

    def run(self):
        """전체 파이프라인을 실행합니다."""
        if not self.track_ids:
            print("처리할 트랙이 없습니다. 파이프라인을 종료합니다.")
            return

        self._setup_csv_file()
        self._start_workers()

        print("--- 트랙 ID를 큐에 추가 시작 ---")
        for tid in self.track_ids:
            self.track_id_queue.put(tid)
        print(f"총 {self.total_tasks}개의 트랙 ID를 큐에 추가했습니다.\n")
        
        print("--- 모든 트랙 ID 전송 완료. 오디오 로더 종료 대기 ---")
        for _ in range(config.NUM_AUDIO_LOADERS):
            self.track_id_queue.put(None)
        for p in self.loader_processes:
            p.join()
        print("--- 모든 오디오 로더 워커가 종료되었습니다. ---")

        print("--- Whisper 워커에게 종료 신호를 보냅니다. ---")
        for _ in range(config.NUM_GPUS):
            self.audio_data_queue.put((None, None))
        
        self._process_results()
        
        print("--- 모든 Whisper 워커 프로세스 종료 대기 ---")
        for p in self.whisper_processes:
            p.join()
        
        print("--- 모든 워커가 성공적으로 종료되었습니다. ---")


if __name__ == "__main__":
    try:
        multiprocessing.set_start_method('spawn', force=True)
        print("✅ multiprocessing 시작 방식을 'spawn'으로 설정했습니다.")
    except RuntimeError as e:
        print(f"❌ multiprocessing 시작 방식 설정 오류: {e}.")

    # 1. 소스 파일 생성
    print("--- 일일 소스 파일 생성을 시작합니다. ---")
    # source_creator = SourceCreator()
    # is_source_created = source_creator.create_source_file()
    is_source_created = "1"
    print("--- 일일 소스 파일 생성 완료. ---\n")

    # 2. 소스 파일이 성공적으로 생성되었을 경우에만 파이프라인 실행
    if is_source_created:
        start_time = time.time()
        print(f"start dtime: {datetime.now(config.KST).strftime('%Y-%m-%d %H:%M:%S')}")

        manager = PipelineManager()
        manager.run()

        end_time = time.time()
        total_duration = timedelta(seconds=end_time - start_time)
        print(f"\nend dtime: {datetime.now(config.KST).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"총 소요 시간: {total_duration}")

        with open(config.RESULT_CSV_FILE, 'a', encoding='utf-8', newline='') as f:
            f.write(f"\n# Total Processing Time: {total_duration}\n")
    else:
        print("소스 파일이 생성되지 않았거나 비어있으므로, 음성 처리 파이프라인을 실행하지 않습니다.")
    
    print("\n프로그램 종료.")
