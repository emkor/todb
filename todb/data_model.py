import json
from datetime import date, time, datetime
from typing import Type, List, Tuple, Dict, Any, Union

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

_CONF_TYPE_TO_CASS_TYPE = {
    "bool": "boolean",
    "string": "varchar",
    "date": "date",
    "time": "time",
    "datetime": "timestamp",
    "int": "int",
    "bigint": "bigint",
    "float": "float",
}


def get_python_type(conf_type: str) -> Type:
    return _CONF_TYPE_TO_PYTHON_TYPE[conf_type]


def get_sql_type(conf_type: str) -> Type[TypeEngine]:
    return _CONF_TYPE_TO_SQL_TYPE[conf_type]


def get_cass_type(conf_type: str) -> str:
    return _CONF_TYPE_TO_CASS_TYPE[conf_type]


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
        self.cass_type = get_cass_type(self.conf_type)


PKEY_AUTOINC = "autoincrement"
PKEY_UUID = "uuid"
PKEY_COLS = "columns"


class PrimaryKeyConf(Model):
    def __init__(self, mode: str, columns: List[str]) -> None:
        self.mode = mode
        self.columns = columns

    def is_clustered(self):
        return len(self.columns) > 1


def parse_model_file(file_path: str) -> Tuple[List[ConfColumn], PrimaryKeyConf, InputFileConfig]:
    columns = []
    with open(file_path, "r", encoding="utf-8") as model_file:
        model_conf = json.load(model_file)
    file_config = InputFileConfig(model_conf.get("file", {}))

    col_names = model_conf.get("columns", {})
    for col_name, col_conf in col_names.items():
        column = ConfColumn(name=col_name, col_index=col_conf["input_file_column"], conf_type=col_conf["type"],
                            nullable=col_conf.get("nullable", True), indexed=col_conf.get("index", False),
                            unique=col_conf.get("unique", False))
        columns.append(column)

    pkey_value = model_conf["primary_key"]
    pkey_conf = _parse_primary_key_config(col_names, pkey_value)

    return columns, pkey_conf, file_config


def _parse_primary_key_config(col_names: List[str], pkey_value: Union[str, List[str]]) -> PrimaryKeyConf:
    pkey_conf = None
    if pkey_value == PKEY_AUTOINC:
        pkey_conf = PrimaryKeyConf(PKEY_AUTOINC, [])
    elif pkey_value == PKEY_UUID:
        pkey_conf = PrimaryKeyConf(PKEY_UUID, [])
    elif isinstance(pkey_value, str):
        if pkey_value in col_names:
            pkey_conf = PrimaryKeyConf(PKEY_COLS, [pkey_value])
        else:
            raise ValueError(
                "Can not define primary key on non-existing column: {} (available columns: {})".format(pkey_value,
                                                                                                       col_names))
    elif isinstance(pkey_value, list):
        if any([c not in col_names for c in pkey_value]):
            raise ValueError(
                "Can not define primary, clustered key from columns: {} (available columns: {})".format(pkey_value,
                                                                                                        col_names))
        else:
            pkey_conf = PrimaryKeyConf(PKEY_COLS, pkey_value)
    return pkey_conf
