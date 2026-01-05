from model.src.audio_loader import AudioLoader
from base import VoiceDetector
import whisper

# VoiceDetector를 임시로 object로 대체합니다. 'base.py'가 있다면 원복해야 합니다.
class WhisperVoiceDetector(object):
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
        """
        transcriptions = self.model.transcribe(audio)
        return transcriptions

if __name__ == "__main__":
    # 이 파일 자체를 테스트하기 위한 로직입니다.
    # 직접 실행 시 경로 문제를 해결하기 위해 sys.path를 조정합니다.
    import sys
    import os
    
    # 프로젝트의 루트 디렉토리(src 폴더의 부모)를 path에 추가합니다.
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    sys.path.insert(0, project_root)

    # 이제 'from src.audio_loader import AudioLoader'가 정상적으로 동작합니다.
    from src.audio_loader import AudioLoader
    
    audio_loader = AudioLoader()
    # 음성이 포함된 트랙 ID로 테스트
    speech_audio = audio_loader.load_audio(track_id=523996880, trim=True) 

    if speech_audio is not None:
        # __init__에 필요한 device 인자를 전달하도록 수정했습니다.
        voice_detector = WhisperVoiceDetector(device="cpu")
        transcriptions = voice_detector.voice_detect(speech_audio)
        
        print("\n--- Transcription Result ---")
        print(f"Language: {transcriptions.get('language')}")
        print(f"Text: {transcriptions.get('text', 'N/A')[:200]}...")
        print("--------------------------")
    else:
        print("오디오 로딩에 실패하여 테스트를 진행할 수 없습니다.")

