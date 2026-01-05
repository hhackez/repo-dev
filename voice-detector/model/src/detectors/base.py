from abc import ABC, abstractmethod


class VoiceDetector(ABC):

    @abstractmethod
    def voice_detect(self, audio, sampling_rate=16000):
        pass
