import json
from datetime import date, time, datetime
from typing import Type, List

from sqlalchemy import BigInteger, Integer, Float, Date, Time, DateTime, Boolean, Unicode
from sqlalchemy.sql.type_api import TypeEngine

from todb.abstract import Model

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


def parse_model_file(file_path: str) -> List[ConfColumn]:
    columns = []
    with open(file_path, "r", encoding="utf-8") as model_file:
        model_conf = json.load(model_file)
    for col_name, col_conf in model_conf.items():
        column = ConfColumn(name=col_name, col_index=col_conf["column_index"], conf_type=col_conf["type"],
                            nullable=col_conf.get("nullable", True), indexed=col_conf.get("index", False),
                            unique=col_conf.get("unique", False))
        columns.append(column)
    return columns
