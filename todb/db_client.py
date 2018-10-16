from typing import List

from todb.data_model import ConfColumn, PrimaryKeyConf

ERROR_MSG = "Called method from abstract class: DbClient"


class DbClient(object):
    def init_table(self, name: str, columns: List[ConfColumn], pkey: PrimaryKeyConf) -> None:
        raise NotImplementedError(ERROR_MSG)

    def insert_into(self, table_name: str, objects: List[List[str]]) -> List[List[str]]:
        raise NotImplementedError(ERROR_MSG)

    def drop_table(self, name: str) -> None:
        raise NotImplementedError(ERROR_MSG)

    def count(self, table_name: str) -> int:
        raise NotImplementedError(ERROR_MSG)
