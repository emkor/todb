from datetime import datetime, date, time
from os import path
from typing import List, Any, Optional

from dateutil.parser import parse
from sqlalchemy import MetaData, Table
from sqlalchemy.engine import Engine

from todb.config import config_from_file
from todb.data_types import parse_model_file, ConfColumn
from todb.db_engine import sql_table_from_columns, get_db_engine
from todb.parsing import CsvParser


def to_db(config_file_name: str, model_file_name: str, input_file_name: str) -> None:
    config = config_from_file(config_file_name)
    print("Parsed config to: {}".format(config))

    columns = parse_model_file(model_file_name)
    print("Parsed model columns: {}".format(columns))

    current_time = datetime.utcnow().replace(microsecond=0).time().isoformat()
    table_name = "todb_{}_{}".format(path.basename(input_file_name), current_time)
    db_engine = get_db_engine(config)

    table = _register_db_table(db_engine, table_name, columns)
    parser = CsvParser(config)
    row_counter = 0
    for cells_in_rows in parser.read_rows_in_chunks(input_file_name):
        row_counter += len(cells_in_rows)
        print("Inserted {} rows...".format(row_counter))
        insert_list_of_dicts = []
        for cells_in_row in cells_in_rows:
            entity_dict = {c.name: _cast_to_sql(c, cells_in_row[c.col_index]) for c in columns}
            insert_list_of_dicts.append(entity_dict)
        db_connection = db_engine.connect()
        try:
            result = db_connection.execute(table.insert(), insert_list_of_dicts)
            db_connection.close()
        except Exception as e:
            db_connection.close()
            print("Could not insert: {} values {}".format(e, insert_list_of_dicts))
            continue


def _register_db_table(db_engine: Engine, table_name: str, columns: List[ConfColumn]) -> Table:
    sql_meta = MetaData()
    table = sql_table_from_columns(sql_meta, table_name, columns)
    sql_meta.create_all(db_engine)
    return table


def _cast_to_sql(column: ConfColumn, value: str) -> Optional[Any]:
    try:
        if column.python_type in (datetime, date, time):
            parsed_time = parse(value)
            if column.python_type == datetime:
                return parsed_time
            elif column.python_type == date:
                return parsed_time.date()
            else:
                return parsed_time.time()
        elif column.python_type == bool:
            return bool(value)  # TODO check for true / false strings
        else:
            return column.python_type(value)
    except Exception as e:
        print("WARNING: Could not cast {} for column {}: {}".format(value, column, e))
        if column.nullable:
            return None
        else:
            raise e
