from typing import List, Dict, Any, Optional

from sqlalchemy import MetaData, Column, Table, select, func, Integer
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.pool import NullPool

from todb.todb_config import ToDbConfig
from todb.data_model import ConfColumn


class SqlClient(object):
    def __init__(self, todb_config: ToDbConfig, db_engine: Optional[Engine] = None) -> None:
        self.todb_config = todb_config
        self._db_engine = db_engine

    def init_table(self, name: str, columns: List[ConfColumn]) -> Table:
        meta = MetaData()
        table = self.get_table(name)
        if table is None:
            print("Creating table named {}...".format(name))
            table = self._sql_table_from_columns(meta, name, columns)
            meta.create_all(self._get_db_engine())
        return table

    def insert_into(self, table: Table, objects: List[Dict[str, Any]]):
        db_connection = self._get_db_engine().connect()
        try:
            db_connection.execute(table.insert(), objects)
            db_connection.close()
        except Exception as e:
            db_connection.close()
            print("Could not insert: {} (objects: {})".format(e, objects))

    def drop_table(self, name: str) -> None:
        the_table = self.get_table(name)
        if the_table is not None:
            the_table.drop(bind=self._get_db_engine())

    def get_table(self, name: str) -> Optional[Table]:
        meta = MetaData()
        meta.reflect(bind=self._get_db_engine())
        try:
            return meta.tables[name]
        except KeyError as e:
            print("Could not find DB table named {}: {}".format(name, e))
            return None

    def count(self, table: Table) -> int:
        db_connection = self._get_db_engine().connect()
        try:
            count = db_connection.scalar(
                select([func.count()]).select_from(table)
            )
        except Exception as e:
            print("Could not count: {}".format(e))
            count = 0
        db_connection.close()
        return count

    def _get_db_engine(self) -> Engine:
        if self._db_engine is None:
            print("Connecting to DB with connection {}".format(self.todb_config.db_url()))
            self._db_engine = create_engine(self.todb_config.db_url(), echo=False, poolclass=NullPool)
        return self._db_engine

    def _sql_table_from_columns(self, sql_metadata: MetaData, table_name: str, columns: List[ConfColumn]) -> Table:
        id_column = Column("id", Integer, primary_key=True, autoincrement=True)
        sql_columns = [Column(c.name, c.sql_type, nullable=c.nullable, index=c.indexed, unique=c.unique)
                       for c in columns]
        return Table(table_name, sql_metadata, id_column, *sql_columns)
