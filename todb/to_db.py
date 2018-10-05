import multiprocessing as mp
from datetime import datetime
from os import path
from typing import List, Any, Dict

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
    sql_client.init_table(table_name, columns)

    print("Inserting data into SQL...")
    parser = CsvParser(config)
    row_counter = 0
    tasks = mp.JoinableQueue(maxsize=2 * config.parsing_concurrency())
    workers = [
        Worker(tasks, EntityBuilder(columns), SqlClient(config), table_name)
        for _ in range(config.parsing_concurrency())
    ]
    [w.start() for w in workers]

    for cells_in_rows in parser.read_rows_in_chunks(input_file_name):
        row_counter += len(cells_in_rows)
        print("Parsed {} rows...".format(row_counter))
        tasks.put(cells_in_rows)
    [tasks.put(None) for w in workers]
    tasks.join()


class Worker(mp.Process):
    def __init__(self, task_queue: mp.Queue, entity_builder: EntityBuilder,
                 sql_client: SqlClient, table_name: str) -> None:
        super(Worker, self).__init__()
        self.table_name = table_name
        self.task_queue = task_queue
        self.entity_builder = entity_builder
        self.sql_client = sql_client

    def run(self):
        print("{} | Starting!".format(self.name))
        while True:
            rows = self.task_queue.get()
            if rows is None:
                print("{} | Exiting!".format(self.name))  # Poison pill means shutdown
                self.task_queue.task_done()
                break
            list_of_model_dicts = self._parse_into_sql_dicts(rows)
            self.sql_client.insert_into(table=self.sql_client.get_table(self.table_name),
                                        objects=list_of_model_dicts)
            self.task_queue.task_done()

    def _parse_into_sql_dicts(self, rows: List[List[str]]) -> List[Dict[str, Any]]:
        list_of_model_dicts = []
        for row_cells in rows:
            entity = self.entity_builder.to_entity(row_cells)
            if entity is not None:
                list_of_model_dicts.append(entity)
        return list_of_model_dicts
