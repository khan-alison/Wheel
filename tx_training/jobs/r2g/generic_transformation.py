import json
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class GenericTransformation:
    def __init__(self, config_file: str):
        with open(config_file, "r") as f:
            self.config = json.load(f)
        self.dfs = {}

    def load_data(self):
        logger.info(self.config["input"])

    def execute(self):
        self.load_data()


if __name__ == "__main__":
    config_file = "job_entries/r2g/config.json"
    generic_transform = GenericTransformation(config_file)
    generic_transform.execute()
