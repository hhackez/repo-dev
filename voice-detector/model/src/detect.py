import os
import pandas as pd
from urllib.parse import urlparse
from collections import Counter
import re
from datetime import datetime
import torch # device 설정을 위해 torch import
import argparse # 인자 처리를 위해 argparse import
import yaml # config 파일(yml) 로드를 위해 yaml import
import boto3
import tempfile # 로컬 임시 파일을 위해 tempfile import

# base.py 임포트를 제거합니다.
from audio_loader import AudioLoader
from whisper_voice_detector import WhisperVoiceDetector


class VoiceDetectionModel:
    """
    Base 클래스를 상속받지 않고, 자체적으로 설정을 관리하며
    오디오 파일의 음성(가사) 유무를 탐지하는 메인 클래스.
    """
    # main.py에서 task 인자를 받도록 __init__ 수정
    def __init__(self, config_path: str, yyyymmdd: str, task: str = "detect"):

        # 설정 파일을 읽어서 config 변수들을 설정합니다.
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
        except FileNotFoundError:
            print(f"[ERROR] 설정 파일({config_path})을 찾을 수 없습니다.")
            raise

        config_footprint = self.config['footprint']
        self.input_config = self.config['input']
        self.output_config = self.config['output']

        # Set device (cuda or cpu)
        if torch.cuda.is_available():
            self.device = "cuda"
        else:
            self.device = "cpu"

        print(f"[INFO] Using device: {self.device}")

        self.yyyymmdd = yyyymmdd # 인자로 받은 날짜를 사용
        self.task_name = task # main.py에서 받은 task 이름 저장
        self.audio_loader = AudioLoader()
        self.voice_detector = WhisperVoiceDetector(device=self.device)

        # s3:// URI가 잘리지 않도록, config의 경로를 그대로 사용합니다.
        self.footprint = config_footprint.rstrip('/')
        print(f"[INFO] >> base footprint: {self.footprint}")

        # [수정] S3 파일 시스템 객체 대신 boto3 클라이언트 초기화
        try:
            self.s3_client = boto3.client("s3")
        except Exception as e:
            print(f"[ERROR] boto3 S3 클라이언트 초기화 실패: {e}")
            print("       -> AWS 자격 증명(credentials) 또는 IAM 역할을 확인하세요.")
            raise

        print(f"[INFO] VoiceDetectionModel (task: {self.task_name}) 초기화 완료.")

    def run(self):
        """음성 탐지 프로세스를 실행합니다."""
        # [수정] S3 경로를 bucket과 key로 분리
        input_dir = self.footprint
        input_s3_path = os.path.join(input_dir, 'create', self.input_config['file_name'])

        try:
            parsed_url = urlparse(input_s3_path)
            s3_bucket = parsed_url.netloc
            s3_key = parsed_url.path.lstrip('/')
            print(f"[INFO] S3 입력 파일 경로: s3://{s3_bucket}/{s3_key}")
        except Exception as e:
            print(f"[ERROR] S3 입력 경로 파싱 실패: {input_s3_path} ({e})")
            return

        local_input_path = None # finally 블록에서 참조할 수 있도록 외부에 선언
        try:
            # --- [수정] boto3를 사용하여 S3 파일을 로컬 임시 파일로 다운로드 ---
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
                local_input_path = tmp.name
            
            print(f"[INFO] S3 입력 파일 다운로드 시작: {input_s3_path}")
            self.s3_client.download_file(s3_bucket, s3_key, local_input_path)
            print(f"[INFO] 로컬 임시 파일로 다운로드 완료: {local_input_path}")
            # --- [수정 완료] ---

            # 1. 로컬 파일을 먼저 로드합니다.
            try:
                # [수정] S3 경로 대신 로컬 임시 파일 경로를 읽습니다.
                track_df = pd.read_csv(local_input_path)
            except FileNotFoundError:
                print(f"[ERROR] 로컬 임시 파일({local_input_path})을 찾을 수 없습니다.")
                return
            except pd.errors.EmptyDataError:
                print(f"[ERROR] 다운로드한 입력 파일({local_input_path})이 비어있습니다.")
                return
            
            # 2. 'track_id' 컬럼이 존재하는지 확인합니다.
            if 'track_id' not in track_df.columns:
                print(f"[ERROR] 입력 파일에 'track_id' 컬럼이 존재하지 않습니다.")
                print(f"       -> 실제 컬럼: {list(track_df.columns)}")
                return
            
            # 3. 컬럼 확인 후 데이터를 추출합니다.
            track_ids = track_df['track_id'].tolist()
            
            if not track_ids:
                print(f"[INFO] 입력 파일에 처리할 track_id가 없습니다.")
                return

            # --- [임시 수정] 테스트를 위해 처리 건수를 제한 (2000건) ---
            # if len(track_ids) > 100:
            #     print(f"[INFO] 테스트 모드: {len(track_ids)}건 중 5,000건만 처리합니다.")
            #     track_ids = track_ids[:100]
            # --- [임시 수정 완료] ---

            results = []
            
            # [수정] 진행률 표시를 위한 enumerate 추가
            for idx, track_id in enumerate(track_ids, start=1):
                progress = (idx / len(track_ids)) * 100
                print(f"[INFO] ({idx}/{len(track_ids)}) [{progress:.2f}%] 트랙 ID {track_id} 처리 시작...")
                
                try:
                    tid = int(track_id)
                except Exception as e:
                    print(f"[ERROR] track_id 형변환 실패: {track_id} ({e}). 이 트랙을 건너뜁니다.")
                    continue 

                audio = self.audio_loader.load_audio(tid, trim=True)

                if audio is None:
                    print(f"[WARNING] 트랙 ID {track_id}의 오디오를 로드할 수 없습니다. 건너뜁니다.")
                    continue

                transcriptions = self.voice_detector.voice_detect(audio)
                
                # Whisper 분석 결과 상세 추출
                text_length = len(transcriptions.get('text', ''))
                segments = transcriptions.get("segments", [])
                all_seg_cnt = len(segments)

                if not segments:
                    processed_data = [track_id, text_length, 0, 0.0, 0.0, 0.0, 0, 0, 0]
                else:
                    new_text_segments = [seg['text'] for seg in segments if seg.get('no_speech_prob', 0) < 0.6]
                    nsp06_seg_cnt = len(new_text_segments)
                    new_text = " ".join(new_text_segments)
                    tokens = re.findall(r'\S+', new_text.lower())
                    nsp06_word_cnt = len(tokens)
                    nsp06_unique_word_cnt = len(Counter(tokens))
                    total_duration = segments[-1]['end']
                    speech_duration = sum(seg['end'] - seg['start'] for seg in segments if seg.get('no_speech_prob', 0) < 0.6)
                    speech_ratio = speech_duration / total_duration if total_duration > 0 else 0
                    
                    processed_data = [
                        track_id, text_length, all_seg_cnt, total_duration,
                        speech_duration, speech_ratio, nsp06_seg_cnt,
                        nsp06_word_cnt, nsp06_unique_word_cnt
                    ]
                
                results.append(processed_data)
                print(f"[INFO] ({idx}/{len(track_ids)}) [{progress:.2f}%] 트랙 ID {track_id} 처리 완료.")

            # 결과 데이터프레임 생성
            columns = [
                'track_id', 'text_length', 'all_seg_cnt', 'total_duration',
                'speech_duration', 'speech_ratio', 'nsp06_seg_cnt',
                'nsp06_word_cnt', 'nsp06_unique_word_cnt'
            ]
            result_df = pd.DataFrame(results, columns=columns)

            # 결과 파일 저장 경로는 S3로 동일
            output_dir = os.path.join(self.footprint, self.task_name)
            base_name, ext = os.path.splitext(self.output_config['file_name'])
            output_filename = f"{base_name}_{self.yyyymmdd}{ext}"
            output_s3_path = os.path.join(output_dir, output_filename)
            
            # [수정] S3 쓰기 전에 총 건수 출력
            print(f"[INFO] 총 {len(results)}건의 트랙 처리 완료.")
            print(f"[INFO] 결과 저장 경로 (S3): {output_s3_path}")
            
            # S3로 직접 쓰기
            result_df.to_csv(output_s3_path, index=False)
            print(f"[INFO] 결과 저장 완료: {output_s3_path}")

        except PermissionError as e:
            # S3fs (fsspec)에서 발생하는 권한 오류를 잡습니다.
            print(f"[ERROR] S3 접근 권한 오류가 발생했습니다: {e}")
            print(f"       -> 읽기 경로: {input_s3_path}")
        except Exception as e:
            print(f"[ERROR] 처리 중 예외 발생: {e}")
        finally:
            # --- [수정] 로컬 임시 파일이 생성되었다면 삭제합니다. ---
            if local_input_path and os.path.exists(local_input_path):
                os.remove(local_input_path)
                print(f"[INFO] 로컬 임시 파일 삭제: {local_input_path}")


    def close(self):
        """
        main.py의 'with closing(...)' 구문을 지원하기 위한 메서드입니다.
        """
        print(f"[INFO] 'detect' task 완료. 리소스를 해제합니다.")
        pass


def main_test():
    """
    이 파일을 직접 실행할 때 사용되는 테스트용 메인 함수입니다.
    main.py에서 실행될 때는 이 함수가 호출되지 않습니다.
    """
    print("--- [detect.py] 자체 테스트 모드 시작 ---")
    
    # 스크립트 위치를 기준으로 기본 config 파일 경로를 계산합니다.
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir) # detect.py가 src/ 안에 있으므로
        default_config_path = os.path.join(project_root, 'res', 'config.detect.dev.yml')
    except NameError:
        default_config_Dpath = 'res/config.detect.dev.yml'

    parser = argparse.ArgumentParser(description="[테스트용] 오디오 파일에서 음성을 탐지합니다.")
    parser.add_argument("--config", type=str, default=default_config_path, help="설정 파일 경로")
    parser.add_argument("--yyyymmdd", type=str, default=datetime.now().strftime("%Y%m%d"), help="처리 날짜 (YYYYMMDD)")
    args = parser.parse_args()

    model = VoiceDetectionModel(
        config_path=args.config,
        yyyymmdd=args.yyyymmdd,
        task="detect" # "detect_test" -> "detect"
    )
    model.run()
    model.close()
    print("--- [detect.py] 자체 테스트 모드 종료 ---")


if __name__ == "__main__":
    main_test()

