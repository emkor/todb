import multiprocessing as mp
from typing import List, Tuple

from todb.config import ToDbConfig
from todb.data_types import ConfColumn
from todb.entity_builder import EntityBuilder
from todb.parsing import CsvParser
from todb.importer import Importer
from todb.sql_client import SqlClient


class ParallelExecutor(object):
    def __init__(self, config: ToDbConfig, columns: List[ConfColumn], table_name: str) -> None:
        self.config = config
        self.columns = columns
        self.table_name = table_name

    def start(self, input_file_name: str) -> Tuple[int, int]:
        sql_client = SqlClient(self.config)
        table = sql_client.init_table(self.table_name, self.columns)

        tasks = mp.JoinableQueue(maxsize=2 * self.config.parsing_concurrency())  # type: ignore
        workers = [
            ParsingWorker(tasks, EntityBuilder(self.columns), SqlClient(self.config), self.table_name)
            for _ in range(self.config.parsing_concurrency())
        ]
        for worker in workers:
            worker.start()

        print("Inserting data into SQL...")
        parser = CsvParser(self.config)
        row_counter = 0
        for cells_in_rows in parser.read_rows_in_chunks(input_file_name):
            row_counter += len(cells_in_rows)
            print("Parsed {} rows...".format(row_counter))
            tasks.put(cells_in_rows)
        [tasks.put(None) for w in workers]
        tasks.join()

        db_row_count = sql_client.count(table)
        return row_counter, db_row_count


class ParsingWorker(mp.Process):
    def __init__(self, task_queue: mp.Queue, entity_builder: EntityBuilder,
                 sql_client: SqlClient, table_name: str) -> None:
        super(ParsingWorker, self).__init__()
        self.importer = Importer(entity_builder, sql_client, table_name)
        self.task_queue = task_queue

    def run(self):
        print("{} | Starting!".format(self.name))
        while True:
            rows = self.task_queue.get()
            if rows is None:
                print("{} | Exiting!".format(self.name))  # Poison pill means shutdown
                self.task_queue.task_done()
                break
            self.importer.parse_and_import(rows)
            self.task_queue.task_done()
