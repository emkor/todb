from datetime import datetime
from os import path
from typing import List

from sqlalchemy import MetaData

from todb.config import ToDbConfig, config_from_file
from todb.data_types import parse_model_file, ConfColumn
from todb.db_engine import sql_table_from_columns, get_db_engine


def to_db(config_file_name: str, model_file_name: str, input_file_name: str) -> None:
    config = config_from_file(config_file_name)
    print("Parsed config to: {}".format(config))

    columns = parse_model_file(model_file_name)
    print("Parsed model columns: {}".format(columns))

    current_time = datetime.utcnow().replace(microsecond=0).time().isoformat()
    table_name = "todb_{}_{}".format(path.basename(input_file_name), current_time)
    _register_db_table(table_name, columns, config)


def _register_db_table(table_name: str, columns: List[ConfColumn], config: ToDbConfig) -> None:
    db_engine = get_db_engine(todb_config=config)
    sql_meta = MetaData()
    sql_table_from_columns(sql_meta, table_name, columns)
    sql_meta.create_all(db_engine)
