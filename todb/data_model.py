import json
from datetime import date, time, datetime
from typing import Type, List, Tuple, Dict, Any

from sqlalchemy import BigInteger, Integer, Float, Date, Time, DateTime, Boolean, Unicode
from sqlalchemy.sql.type_api import TypeEngine

from todb.abstract import Model

DEFAULT_FILE_ENCODING = "utf-8"
DEFAULT_HAS_HEADER_ROW = True
DEFAULT_ROW_DELIMITER = "\n"
DEFAULT_CELL_DELIMITER = ","


class InputFileConfig(Model):
    def __init__(self, conf_dict: Dict[str, Any]) -> None:
        self.conf_dict = conf_dict

    def file_encoding(self) -> str:
        return str(self.conf_dict.get("encoding", DEFAULT_FILE_ENCODING))

    def has_header_row(self) -> bool:
        return bool(self.conf_dict.get("has_header_row", DEFAULT_HAS_HEADER_ROW))

    def row_delimiter(self) -> str:
        return str(self.conf_dict.get("row_delimiter", DEFAULT_ROW_DELIMITER))

    def cell_delimiter(self) -> str:
        return str(self.conf_dict.get("cell_delimiter", DEFAULT_CELL_DELIMITER))


_CONF_TYPE_TO_PYTHON_TYPE = {
    "bool": bool,
    "string": str,
    "date": date,
    "time": time,
    "datetime": datetime,
    "int": int,
    "bigint": int,
    "float": float,
}

_CONF_TYPE_TO_SQL_TYPE = {
    "bool": Boolean,
    "string": Unicode,
    "date": Date,
    "time": Time,
    "datetime": DateTime,
    "int": Integer,
    "bigint": BigInteger,
    "float": Float,
}


def get_python_type(conf_type: str) -> Type:
    return _CONF_TYPE_TO_PYTHON_TYPE[conf_type]


def get_sql_type(conf_type: str) -> Type[TypeEngine]:
    return _CONF_TYPE_TO_SQL_TYPE[conf_type]


class ConfColumn(Model):
    def __init__(self, name: str, col_index: int, conf_type: str,
                 nullable: bool, indexed: bool, unique: bool) -> None:
        self.name = name
        self.col_index = col_index
        self.nullable = nullable
        self.indexed = indexed
        self.unique = unique
        self.conf_type = conf_type
        self.python_type = get_python_type(self.conf_type)
        self.sql_type = get_sql_type(self.conf_type)

    def is_key(self) -> bool:
        return bool(self.indexed and self.unique)


def parse_model_file(file_path: str) -> Tuple[List[ConfColumn], InputFileConfig]:
    columns = []
    with open(file_path, "r", encoding="utf-8") as model_file:
        model_conf = json.load(model_file)
    for col_name, col_conf in model_conf.get("columns", {}).items():
        column = ConfColumn(name=col_name, col_index=col_conf["input_file_column"], conf_type=col_conf["type"],
                            nullable=col_conf.get("nullable", True), indexed=col_conf.get("index", False),
                            unique=col_conf.get("unique", False))
        columns.append(column)
    file_config = InputFileConfig(model_conf.get("file", {}))
    return columns, file_config
