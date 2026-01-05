import io
import torchaudio
import requests
#import tempfile # tempfile은 이제 사용되지 않지만, 기존 import를 남겨둡니다.
#import os # os도 마찬가지입니다.
import torch
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
# pydub import 추가
from pydub import AudioSegment
# numpy import 추가
import numpy as np

timestamp = datetime.now().strftime('%Y%Y%m%d_%H%M%S')
# filename = f"logs/result_data_{timestamp}.json"
# error_tracks = f"logs/error_tracks_{timestamp}.txt"

class AudioLoader:
    def __init__(self):
        self.target_sr = 16000
        self.target_duration_sec = 60  # 1분
        # self.cdn_url = "https://pri-alice.qa01.music-flo.com/reco/tracks"
        self.cdn_url = "https://pri-alice.prod.music-flo.com/reco/tracks"

    def _get_audio_cdn_url(self, track_id):
        try:
            res = requests.get(f"{self.cdn_url}/{track_id}")
            if res.status_code == 200:
                url = res.json()['data']['url']
                return url
            else:
                raise Exception(f"mp3 url을 가져올 수 없습니다. {res.json()}")
        except Exception as e:
            raise e

    def _load_audio_from_url(self, url):
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"오디오 파일 다운로드 실패: {response.status_code}")

        audio_data = response.content

        # --- [수정] pydub을 사용한 수동 텐서 변환 (torchaudio.load 우회) ---
        try:
            # 1. 원본 오디오 데이터를 메모리 버퍼(BytesIO)로 래핑
            input_buffer = io.BytesIO(audio_data)

            # 2. pydub이 메모리 버퍼에서 직접 오디오 로드
            print(f"[INFO] pydub: 메모리 버퍼에서 오디오 로드 시도...")
            audio = AudioSegment.from_file(input_buffer)
            print(f"[INFO] pydub: 오디오 로드 성공.")

            # 3. 오디오를 목표 포맷(16kHz, mono, 16-bit)으로 변환
            audio = audio.set_frame_rate(self.target_sr)
            audio = audio.set_channels(1)
            audio = audio.set_sample_width(2) # 16-bit PCM
            print(f"[INFO] pydub: 오디오 {self.target_sr}Hz, 1채널(mono)로 변환 완료.")

            # 4. pydub에서 raw PCM 데이터를 bytes로 추출
            raw_data = audio.raw_data
            
            # 5. bytes를 NumPy 배열(int16)로 변환
            np_samples = np.frombuffer(raw_data, dtype=np.int16)
            
            # 6. NumPy 배열을 PyTorch 텐서(float32)로 변환하고 정규화
            #    int16 범위(-32768 ~ 32767) -> float32 범위(-1.0 ~ 1.0)
            wave_form = torch.tensor(np_samples, dtype=torch.float32) / 32768.0
            
            # 7. torchaudio.load()와 동일한 샘플링 레이트(sr) 변수 생성
            sr = self.target_sr 
            print(f"[INFO] Pytorch 텐서 변환 성공. (Shape: {wave_form.shape})")

        except Exception as e:
            print(f"[ERROR] pydub 변환 또는 텐서 변환 실패: {e}")
            raise
            
        finally:
            # 임시 파일을 사용하지 않으므로 삭제 로직 필요 없음
            pass
        # --- [수정 완료] ---

        # [수정] pydub을 통해 이미 모노(1D 텐서)로 변환되었으므로,
        #        채널 평균 계산 로직은 필요하지 않습니다. (주석 처리)
        # if wave_form.size(0) > 1:
        #     wave_form = wave_form.mean(dim=0, keepdim=True)

        if sr != self.target_sr:
            # pydub에서 self.target_sr로 강제 변환했으므로 이 로직은 실행되지 않아야 합니다.
            print(f"[WARNING] 샘플링 레이트 불일치: {sr} vs {self.target_sr}. 리샘플링 실행.")
            transform = torchaudio.transforms.Resample(orig_freq=sr, new_freq=self.target_sr)
            wave_form = transform(wave_form)

        return wave_form.squeeze()
    
    def slice_source(self, waveform, sample_rate):
        """
        - 90초 미만: 그대로 반환
        - 90초 이상: 전체를 1/3, 2/3 지점에서 각 30초씩 잘라서 총 1분 반환
        """
        total_samples = waveform.size(0)
        keep_threshold_samples = 90 * sample_rate   # 90초 기준
        slice_samples = 30 * sample_rate            # 30초씩 자를 양

        if total_samples < keep_threshold_samples:
            return waveform  # 90초 미만이면 그대로 반환

        # 1/3, 2/3 지점에서 자르기
        first_start = int(total_samples / 3)
        second_start = int((2 * total_samples) / 3)

        # 범위 초과 방지
        if first_start + slice_samples > total_samples or second_start + slice_samples > total_samples:
            # [수정] ValueError 대신 경고를 출력하고, 가능한 최대치로 슬라이스 (에러 방지)
            print(f"[WARNING] 오디오 길이가 슬라이싱에 충분하지 않아 마지막 30초*2를 사용합니다.")
            part1 = waveform[-slice_samples*2 : -slice_samples]
            part2 = waveform[-slice_samples:]
            if part1.size(0) < slice_samples or part2.size(0) < slice_samples:
                print(f"[ERROR] 슬라이싱 실패: 오디오 길이가 너무 짧습니다.")
                return waveform # 슬라이싱 실패 시 원본 반환
        else:
            part1 = waveform[first_start:first_start + slice_samples]
            part2 = waveform[second_start:second_start + slice_samples]

        return torch.cat([part1, part2])


    def load_audio(self, track_id, trim=False):
        try:
            audio_cdn_url = self._get_audio_cdn_url(track_id)
            
            # --- 디버깅 코드 추가 ---
            print(f"[DEBUG] Track {track_id}의 CDN URL: {audio_cdn_url}")
            # --- 디버깅 코드 끝 ---
            
            waveform = self._load_audio_from_url(audio_cdn_url)

            if trim:
                waveform = self.slice_source(waveform, self.target_sr)

            return waveform
    
        except Exception as e:
            print(f"[ERROR] track_id {track_id} 처리 중 오류 발생: {e}")
            # with open(error_tracks, "a) as f:
            #     f.write(f"{track_id}\n")
            return None  # 계속 진행할 수 있도록 None 반환

if __name__ == "__main__":
    audio_loader = AudioLoader()

    track_id = 517156106
    
    # print("torchaudio backend:", torchaudio.get_audio_backend())

    audio_full = audio_loader.load_audio(track_id=track_id, trim=False)
    if audio_full is not None:
        print("Full audio length:", audio_full.shape)
    else:
        print(f"[SKIP] {track_id} full audio 로딩 실패")

    audio_trimmed = audio_loader.load_audio(track_id=track_id, trim=True)
    if audio_trimmed is not None:
        print("Trimmed audio length (1min max):", audio_trimmed.shape)
    else:
        print(f"[SKIP] {track_id} trimmed audio 로딩 실패")

