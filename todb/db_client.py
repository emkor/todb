from typing import List, Tuple

from todb.data_model import ConfColumn, PrimaryKeyConf

ERROR_MSG = "Called method from abstract class: DbClient"


class DbClient(object):
    def init_table(self, name: str, columns: List[ConfColumn], pkey: PrimaryKeyConf) -> None:
        raise NotImplementedError(ERROR_MSG)

    def insert_in_batch(self, table_name: str, objects: List[List[str]]) -> Tuple[bool, List[List[str]]]:
        raise NotImplementedError(ERROR_MSG)

    def insert_one_by_one(self, table_name: str, objects: List[List[str]]) -> List[List[str]]:
        raise NotImplementedError(ERROR_MSG)

    def drop_table(self, name: str) -> None:
        raise NotImplementedError(ERROR_MSG)

    def count(self, table_name: str) -> int:
        raise NotImplementedError(ERROR_MSG)
