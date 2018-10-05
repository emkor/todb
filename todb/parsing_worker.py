import multiprocessing as mp
from typing import List, Any, Dict

from todb.entity_builder import EntityBuilder
from todb.sql_client import SqlClient


class ParsingWorker(mp.Process):
    def __init__(self, task_queue: mp.Queue, entity_builder: EntityBuilder,
                 sql_client: SqlClient, table_name: str) -> None:
        super(ParsingWorker, self).__init__()
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
