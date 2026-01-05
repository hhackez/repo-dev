# from audio_loader import AudioLoader
# from detectors.base import VoiceDetector
# import whisperx
#
#
# class WhisperXVoiceDetector(VoiceDetector):
#     def __init__(self,
#                  model_name="base",
#                  device="cuda",
#                  compute_type="float16"):
#         self.model = whisperx.load_model(
#             model_name,
#             device=device,
#             compute_type=compute_type
#         )
#
#     def voice_detect(self, audio, sampling_rate=16000):
#         transcriptions = self.model.transcribe(audio.numpy())
#         results = []
#         for segment in transcriptions["segments"]:
#             results.append(segment['end'] - segment['start'])
#         return sum(results) > 20, transcriptions
#
#
# if __name__ == "__main__":
#     audio_loader = AudioLoader()
#     speech_audio = audio_loader.load_audio(track_id=509432093)
#
#     voice_detector = WhisperXVoiceDetector()
#     result, transcriptions = voice_detector.voice_detect(speech_audio)
#     print(f"speech_audio에 음성이 있습니까? {result}")
#
#     # non_speech_audio = audio_loader.load_audio(track_id=1117587)
#     # result = voice_detector.voice_detect(non_speech_audio)
#     # print(f"non_speech_audio에 음성이 있습니까? {result}")

# 이 코드로는 성능 테스트 해보지 않음
# whisper보다 빠르다고 평가 되고 있음
# https://github.com/m-bain/whisperX