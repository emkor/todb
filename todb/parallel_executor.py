import multiprocessing as mp
from typing import List, Tuple

from todb.fail_row_handler import FailRowHandler
from todb.params import InputParams
from todb.data_model import ConfColumn, InputFileConfig
from todb.entity_builder import EntityBuilder
from todb.parsing import CsvParser
from todb.importer import Importer
from todb.sql_client import SqlClient


class ParallelExecutor(object):
    def __init__(self, params: InputParams, input_file_config: InputFileConfig, columns: List[ConfColumn],
                 table_name: str, failed_rows_file: str) -> None:
        self.params = params
        self.input_file_config = input_file_config
        self.columns = columns
        self.table_name = table_name
        self.failed_rows_file = failed_rows_file

    def start(self, input_file_name: str) -> Tuple[int, int]:
        sql_client = SqlClient(self.params.sql_db)
        sql_client.init_table(self.table_name, self.columns)
        initial_row_count = sql_client.count(self.table_name)

        unsuccessful_rows_queue = mp.JoinableQueue(maxsize=2 * self.params.processes)  # type: ignore
        fail_row_handler = FailRowHandler(self.input_file_config, self.failed_rows_file)
        failure_handling_worker = UnsuccessfulRowsHandlingWorker(unsuccessful_rows_queue, fail_row_handler)
        failure_handling_worker.start()

        tasks_queue = mp.JoinableQueue(maxsize=2 * self.params.processes)  # type: ignore
        parser_workers = [
            ParsingWorker(tasks_queue, unsuccessful_rows_queue,
                          EntityBuilder(self.columns), SqlClient(self.params.sql_db), self.table_name)
            for _ in range(self.params.processes)
        ]
        for w in parser_workers:
            w.start()

        print("Inserting data into SQL...")
        parser = CsvParser(self.input_file_config, self.params.chunk_size_kB)
        row_counter = 0
        for cells_in_rows in parser.read_rows_in_chunks(input_file_name):
            row_counter += len(cells_in_rows)
            print("Parsed {} rows ({} so far)...".format(len(cells_in_rows), row_counter))
            tasks_queue.put(cells_in_rows)
        print("Waiting till values will be stored in DB...")
        [tasks_queue.put(None) for w in parser_workers]
        tasks_queue.join()

        print("Waiting till failed rows will be stored in file...")
        unsuccessful_rows_queue.put(None)
        unsuccessful_rows_queue.join()

        return row_counter, sql_client.count(self.table_name) - initial_row_count


class ParsingWorker(mp.Process):
    def __init__(self, task_queue: mp.Queue, unsuccessful_rows_queue: mp.Queue,
                 entity_builder: EntityBuilder, sql_client: SqlClient, table_name: str) -> None:
        super(ParsingWorker, self).__init__()
        self.importer = Importer(entity_builder, sql_client, table_name)
        self.task_queue = task_queue
        self.unsuccessful_rows_queue = unsuccessful_rows_queue

    def run(self):
        print("{} | ParsingWorker starting!".format(self.name))
        while True:
            rows = self.task_queue.get()
            if rows is None:
                print("{} | ParsingWorker exiting!".format(self.name))  # Poison pill means shutdown
                self.task_queue.task_done()
                break
            unsuccessful_rows = self.importer.parse_and_import(rows)
            if unsuccessful_rows:
                self.unsuccessful_rows_queue.put(unsuccessful_rows)
            self.task_queue.task_done()


class UnsuccessfulRowsHandlingWorker(mp.Process):
    def __init__(self, unsuccessful_rows_queue: mp.Queue, handler: FailRowHandler) -> None:
        super(UnsuccessfulRowsHandlingWorker, self).__init__()
        self.unsuccessful_rows_queue = unsuccessful_rows_queue
        self.handler = handler

    def run(self):
        print("{} | UnsuccessfulRowsHandlingWorker starting!".format(self.name))
        while True:
            rows = self.unsuccessful_rows_queue.get()
            if rows is None:
                print("{} | UnsuccessfulRowsHandlingWorker exiting!".format(self.name))  # Poison pill means shutdown
                self.unsuccessful_rows_queue.task_done()
                break
            self.handler.handle_failed_rows(rows)
            self.unsuccessful_rows_queue.task_done()
