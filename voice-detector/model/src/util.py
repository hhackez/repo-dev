import pendulum
import yaml


class Config:
    """
    yaml config parser class
    """

    def __init__(self, config_path: str) -> None:
        self.config = {}
        with open(config_path, encoding="utf-8") as file_fd:
            for key, value in yaml.load(file_fd, Loader=yaml.FullLoader).items():
                self.config[key] = value

    def __getitem__(self, key):
        return self.config[key]

    def __setitem__(self, key, value):
        self.config[key] = value

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return str(self.config)


def get_yyyyymmdd() -> str:
    return pendulum.now("Asia/Seoul").format("YYYYMMDD")
