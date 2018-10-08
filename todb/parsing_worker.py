import multiprocessing as mp
from typing import List

from todb.entity_builder import EntityBuilder
from todb.sql_client import SqlClient


class Importer(object):
    def __init__(self, entity_builder: EntityBuilder, sql_client: SqlClient, table_name: str) -> None:
        self.entity_builder = entity_builder
        self.sql_client = sql_client
        self.table_name = table_name

    def parse_and_import(self, rows: List[List[str]]) -> None:
        list_of_model_dicts = []
        for row_cells in rows:
            entity = self.entity_builder.to_entity(row_cells)
            if entity is not None:
                list_of_model_dicts.append(entity)
        self.sql_client.insert_into(table=self.sql_client.get_table(self.table_name),
                                    objects=list_of_model_dicts)


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
