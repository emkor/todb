import multiprocessing as mp
from datetime import datetime
from os import path
from typing import List

from todb.config import config_from_file, ToDbConfig
from todb.data_types import parse_model_file, ConfColumn
from todb.entity_builder import EntityBuilder
from todb.parsing import CsvParser
from todb.parsing_worker import ParsingWorker
from todb.sql_client import SqlClient


def to_db(config_file_name: str, model_file_name: str, input_file_name: str) -> None:
    config = config_from_file(config_file_name)
    print("Parsed config to: {}".format(config))

    columns = parse_model_file(model_file_name)
    print("Parsed model columns: {}".format(columns))

    current_time = datetime.utcnow().replace(microsecond=0).time().isoformat()
    table_name = "todb_{}_{}".format(path.basename(input_file_name), current_time)
    executor = ParallelExecutor(config, columns, table_name)
    executor.start(input_file_name)


class ParallelExecutor(object):
    def __init__(self, config: ToDbConfig, columns: List[ConfColumn], table_name: str):
        self.config = config
        self.columns = columns
        self.table_name = table_name

    def start(self, input_file_name: str):
        print("Initializing SQL table: {}".format(self.table_name))
        sql_client = SqlClient(self.config)
        sql_client.init_table(self.table_name, self.columns)
        print("Inserting data into SQL...")
        parser = CsvParser(self.config)
        row_counter = 0
        tasks = mp.JoinableQueue(maxsize=2 * self.config.parsing_concurrency())  # type: ignore
        workers = [
            ParsingWorker(tasks, EntityBuilder(self.columns), SqlClient(self.config), self.table_name)
            for _ in range(self.config.parsing_concurrency())
        ]
        for worker in workers:
            worker.start()

        for cells_in_rows in parser.read_rows_in_chunks(input_file_name):
            row_counter += len(cells_in_rows)
            print("Parsed {} rows...".format(row_counter))
            tasks.put(cells_in_rows)
        [tasks.put(None) for w in workers]
        tasks.join()
