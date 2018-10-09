import json
from typing import Any, Text, Dict

from todb.abstract import Model

DEFAULT_PARSING_BUFFER_SIZE_kB = 500
DEFAULT_PARSING_CONCURRENCY = 4
DEFAULT_DB_URL = "sqlite:///"


class ToDbConfig(Model):
    def __init__(self, conf_dict: Dict[Text, Any]) -> None:
        self.conf_dict = conf_dict

    def chunk_size_kB(self) -> float:
        return float(self.conf_dict.get("parsing", {}).get("chunk_size_kB", DEFAULT_PARSING_BUFFER_SIZE_kB))

    def processes(self) -> int:
        return int(self.conf_dict.get("parsing", {}).get("processes", DEFAULT_PARSING_CONCURRENCY))

    def db_url(self) -> str:
        return str(self.conf_dict.get("db_url", DEFAULT_DB_URL))


def config_from_file(file_path: str) -> ToDbConfig:
    with open(file_path, "r") as json_file:
        config_dict = json.load(json_file)
    return ToDbConfig(config_dict)


DEFAULT_FILE_ENCODING = "utf-8"
DEFAULT_HAS_HEADER = True
DEFAULT_ROW_DELIMITER = "\n"
DEFAULT_CELL_DELIMITER = ","


class InputFileConfig(Model):
    def __init__(self, conf_dict: Dict[Text, Any]) -> None:
        self.conf_dict = conf_dict

    def file_encoding(self) -> str:
        return str(self.conf_dict.get("encoding", DEFAULT_FILE_ENCODING))

    def has_header(self) -> bool:
        return bool(self.conf_dict.get("has_header", DEFAULT_HAS_HEADER))

    def row_delimiter(self) -> str:
        return str(self.conf_dict.get("row_delimiter", DEFAULT_ROW_DELIMITER))

    def cell_delimiter(self) -> str:
        return str(self.conf_dict.get("cell_delimiter", DEFAULT_CELL_DELIMITER))
