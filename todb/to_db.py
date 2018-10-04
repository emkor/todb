from datetime import datetime
from os import path

from todb.config import config_from_file
from todb.data_types import parse_model_file
from todb.entity_builder import EntityBuilder
from todb.parsing import CsvParser
from todb.sql_client import SqlClient


def to_db(config_file_name: str, model_file_name: str, input_file_name: str) -> None:
    config = config_from_file(config_file_name)
    print("Parsed config to: {}".format(config))

    columns = parse_model_file(model_file_name)
    print("Parsed model columns: {}".format(columns))

    current_time = datetime.utcnow().replace(microsecond=0).time().isoformat()
    table_name = "todb_{}_{}".format(path.basename(input_file_name), current_time)
    print("Initializing SQL table: {}".format(table_name))
    sql_client = SqlClient(config)
    table = sql_client.init_table(table_name, columns)

    print("Inserting data into SQL...")
    parser = CsvParser(config)
    entity_builder = EntityBuilder(columns)
    row_counter = 0
    for cells_in_rows in parser.read_rows_in_chunks(input_file_name):
        list_of_model_dicts = []
        for row_cells in cells_in_rows:
            entity = entity_builder.to_entity(row_cells)
            if entity is not None:
                list_of_model_dicts.append(entity)
        sql_client.insert_into(table, objects=list_of_model_dicts)
        row_counter += len(cells_in_rows)
        print("Inserted {} rows...".format(row_counter))
