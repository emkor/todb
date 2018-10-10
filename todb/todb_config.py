import json
from typing import Any, Text, Dict

from todb.abstract import Model

DEFAULT_PARSING_BUFFER_SIZE_kB = 500
DEFAULT_PARSING_CONCURRENCY = 4
DEFAULT_DB_URL = "sqlite:///"

MAX_CHUNK_SIZE = 64000
MIN_CHUNK_SIZE = 1
MIN_CONCURRENCY = 1
MAX_CONCURRENCY = 128


class ToDbConfig(Model):
    def __init__(self, conf_dict: Dict[Text, Any]) -> None:
        self.conf_dict = conf_dict

    def chunk_size_kB(self) -> float:
        file_chunk_size = float(self.conf_dict.get("parsing", {}).get("chunk_size_kB", DEFAULT_PARSING_BUFFER_SIZE_kB))
        return min(max(file_chunk_size, MIN_CHUNK_SIZE), MAX_CHUNK_SIZE)

    def processes(self) -> int:
        file_processes = self.conf_dict.get("parsing", {}).get("processes", DEFAULT_PARSING_CONCURRENCY)
        return min(max(int(file_processes), MIN_CONCURRENCY), MAX_CONCURRENCY)

    def db_url(self) -> str:
        return str(self.conf_dict.get("db_url", DEFAULT_DB_URL))


def config_from_file(file_path: str) -> ToDbConfig:
    with open(file_path, "r") as json_file:
        config_dict = json.load(json_file)
    return ToDbConfig(config_dict)
