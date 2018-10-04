import json
from typing import Any, Text, Dict

from todb.abstract import Model

DEFAULT_FILE_ENCODING = "utf-8"
DEFAULT_HAS_HEADER = True
DEFAULT_ROW_DELIMITER = "\n"
DEFAULT_CELL_DELIMITER = ","
DEFAULT_PARSING_BUFFER_SIZE_kB = 1000
DEFAULT_PARSING_CONCURRENCY = 4
DEFAULT_DB_TYPE = "sqlite"
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_PORT = 3306
DEFAULT_DB_USER = "user"
DEFAULT_DB_PASSWORD = "password"
DEFAULT_DB_DATABASE = "default"


class ToDbConfig(Model):
    @classmethod
    def from_file(cls, file_path: str) -> Model:
        with open(file_path, "r") as json_file:
            config_dict = json.load(json_file)
        return ToDbConfig(config_dict)

    def __init__(self, conf_dict: Dict[Text, Any]) -> None:
        self.conf_dict = conf_dict

    def file_encoding(self) -> str:
        return str(self.conf_dict.get("file", {}).get("encoding", DEFAULT_FILE_ENCODING))

    def has_header(self) -> bool:
        return bool(self.conf_dict.get("file", {}).get("has_header", DEFAULT_HAS_HEADER))

    def row_delimiter(self) -> str:
        return str(self.conf_dict.get("file", {}).get("row_delimiter", DEFAULT_ROW_DELIMITER))

    def cell_delimiter(self) -> str:
        return str(self.conf_dict.get("file", {}).get("cell_delimiter", DEFAULT_CELL_DELIMITER))

    def parsing_buffer_size_kB(self) -> float:
        return float(self.conf_dict.get("parsing", {}).get("buffer_size_kB", DEFAULT_PARSING_BUFFER_SIZE_kB))

    def parsing_concurrency(self) -> int:
        return int(self.conf_dict.get("parsing", {}).get("concurrency", DEFAULT_PARSING_CONCURRENCY))

    def db_type(self) -> str:
        return str(self.conf_dict.get("db", {}).get("type", DEFAULT_DB_TYPE))

    def db_host(self) -> str:
        return str(self.conf_dict.get("db", {}).get("host", DEFAULT_DB_HOST))

    def db_port(self) -> int:
        return int(self.conf_dict.get("db", {}).get("port", DEFAULT_DB_PORT))

    def db_user(self) -> str:
        return str(self.conf_dict.get("db", {}).get("user", DEFAULT_DB_USER))

    def db_password(self) -> str:
        return str(self.conf_dict.get("db", {}).get("password", DEFAULT_DB_PASSWORD))

    def db_database(self) -> str:
        return str(self.conf_dict.get("db", {}).get("database", DEFAULT_DB_DATABASE))
