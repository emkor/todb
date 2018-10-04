from typing import Any, Text, Dict


class ToDbConfig(object):
    DEFAULT_FILE_ENCODING = "utf-8"
    DEFAULT_HAS_HEADER = True
    DEFAULT_ROW_DELIMITER = "\n"
    DEFAULT_CELL_DELIMITER = ","
    DEFAULT_PARSING_BUFFER_SIZE_kB = 1000
    DEFAULT_PARSING_CONCURRENCY = 4
    DEFAULT_DB_HOST = "localhost"
    DEFAULT_DB_PORT = 3306
    DEFAULT_DB_USER = "user"
    DEFAULT_DB_PASSWORD = "password"

    def __init__(self, conf_dict: Dict[Text, Any]) -> None:
        self.conf_dict = conf_dict

    def file_encoding(self) -> str:
        return str(self.conf_dict.get("file", {}).get("encoding", self.DEFAULT_FILE_ENCODING))

    def has_header(self) -> bool:
        return bool(self.conf_dict.get("file", {}).get("has_header", self.DEFAULT_HAS_HEADER))

    def row_delimiter(self) -> str:
        return str(self.conf_dict.get("file", {}).get("row_delimiter", self.DEFAULT_ROW_DELIMITER))

    def cell_delimiter(self) -> str:
        return str(self.conf_dict.get("file", {}).get("cell_delimiter", self.DEFAULT_CELL_DELIMITER))

    def parsing_buffer_size_kB(self) -> float:
        return float(self.conf_dict.get("parsing", {}).get("buffer_size_kB", self.DEFAULT_PARSING_BUFFER_SIZE_kB))

    def parsing_concurrency(self) -> int:
        return int(self.conf_dict.get("parsing", {}).get("concurrency", self.DEFAULT_PARSING_CONCURRENCY))

    def db_host(self) -> str:
        return str(self.conf_dict.get("db", {}).get("host", self.DEFAULT_DB_HOST))

    def db_port(self) -> int:
        return int(self.conf_dict.get("db", {}).get("port", self.DEFAULT_DB_PORT))

    def db_user(self) -> str:
        return str(self.conf_dict.get("db", {}).get("user", self.DEFAULT_DB_USER))

    def db_password(self) -> str:
        return str(self.conf_dict.get("db", {}).get("password", self.DEFAULT_DB_PASSWORD))
