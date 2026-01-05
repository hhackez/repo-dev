# whisper_voice_detector.py

from abc import ABC, abstractmethod
import whisper
import sys
import os

# --- base.py의 내용 시작 ---

class VoiceDetector(ABC):

    @abstractmethod
    def voice_detect(self, audio, sampling_rate=16000):
        pass

# --- base.py의 내용 끝 ---


# --- whisper_voice_detector.py의 내용 시작 ---

# VoiceDetector가 이제 이 파일 안에 정의되었으므로 상속받습니다.
class WhisperVoiceDetector(VoiceDetector):
    def __init__(self, device: str):
        """
        지정된 장치(device)에 Whisper 모델을 로드합니다.
        :param device: 모델을 로드할 장치 (예: 'cuda:0', 'cpu')
        """
        # logging.info 대신 print 문으로 변경
        print(f"[INFO] Whisper 모델(base)을 {device}에 로드합니다.")
        self.model = whisper.load_model("base", device=device)

    def voice_detect(self, audio, sampling_rate=16000) -> dict:
        """
        오디오를 받아 텍스트로 변환한 결과를 반환합니다.
        VoiceDetector 추상 메소드를 구현합니다.
        """
        transcriptions = self.model.transcribe(audio)
        return transcriptions

if __name__ == "__main__":
    # 이 파일 자체를 테스트하기 위한 로직입니다.
    # 직접 실행 시 경로 문제를 해결하기 위해 sys.path를 조정합니다.
    
    # 프로젝트의 루트 디렉토리(src 폴더의 부모)를 path에 추가합니다.
    # __file__은 이 스크립트 파일의 경로입니다.
    # os.path.dirname(__file__) -> 현재 파일이 있는 디렉토리
    # os.path.join(..., '..', '..') -> 두 단계 상위 디렉토리
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
      
    project_root_for_test = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    sys.path.insert(0, project_root_for_test)
    
    print(f"[TEST] 프로젝트 루트 경로 추가: {project_root_for_test}")
    print(f"[TEST] sys.path: {sys.path[:3]}...") # 상위 3개 경로만 확인

    # 이제 'from src.audio_loader import AudioLoader'가 정상적으로 동작합니다.
    try:
        from src.audio_loader import AudioLoader
    except ImportError as e:
        print(f"[ERROR] 'src.audio_loader'를 임포트할 수 없습니다. sys.path 문제일 수 있습니다.")
        print(f"에러: {e}")
        print("테스트를 종료합니다.")
        sys.exit(1) # 오류로 종료
    
    audio_loader = AudioLoader()
    # 음성이 포함된 트랙 ID로 테스트
    print("[TEST] 오디오 로드를 시도합니다 (track_id=523996880)...")
    speech_audio = audio_loader.load_audio(track_id=523996880, trim=True) 

    if speech_audio is not None:
        print("[TEST] 오디오 로드 성공. VoiceDetector를 'cpu'로 초기화합니다.")
        # __init__에 필요한 device 인자를 전달하도록 수정했습니다.
        voice_detector = WhisperVoiceDetector(device="cpu")
        print("[TEST] 음성 감지(transcribe)를 시작합니다...")
        transcriptions = voice_detector.voice_detect(speech_audio)
        print("[TEST] 음성 감지 완료.")
        
        print("\n--- Transcription Result ---")
        print(f"Language: {transcriptions.get('language')}")
        # 텍스트가 너무 길 수 있으므로 앞부분 200자만 출력
        print(f"Text: {transcriptions.get('text', 'N/A')[:200]}...")
        print("--------------------------")
    else:
        print("[TEST] 오디오 로딩에 실패하여 테스트를 진행할 수 없습니다.")

# --- whisper_voice_detector.py의 내용 끝 ---