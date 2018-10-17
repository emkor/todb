from typing import List

from todb.db_client import DbClient
from todb.logger import get_logger
from todb.util import split_in_half

INSERT_ONE_BY_ONE_THRESHOLD = 8


class Importer(object):
    def __init__(self, db_client: DbClient) -> None:
        self.db_client = db_client
        self.logger = get_logger()

    def parse_and_import(self, table_name: str, rows: List[List[str]]) -> List[List[str]]:
        """Parses rows and tries to inserts them to DB; returns list of rows that failed to import"""
        if len(rows) <= INSERT_ONE_BY_ONE_THRESHOLD:
            self.logger.debug("Switching to one-by-one mode for {} rows".format(len(rows)))
            return self.db_client.insert_one_by_one(table_name, rows)
        else:
            mass_insert_successful, failed_rows = self.db_client.insert_in_batch(table_name, rows)
            if mass_insert_successful:
                return failed_rows
            else:
                rows_a, rows_b = split_in_half(rows)
                failed_rows_a = self.parse_and_import(table_name, rows_a)
                failed_rows_b = self.parse_and_import(table_name, rows_b)
                return failed_rows_a + failed_rows_b
