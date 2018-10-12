from typing import List, Dict, Any, Optional

from sqlalchemy import MetaData, Column, Table, select, func, Integer
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.pool import NullPool

from todb.data_model import ConfColumn
from todb.db_client import DbClient


class SqlClient(DbClient):
    def __init__(self, db_url: str, db_engine: Optional[Engine] = None) -> None:
        self.db_url = db_url
        self._db_engine = db_engine

    def init_table(self, name: str, columns: List[ConfColumn]) -> None:
        meta = MetaData()
        table = self._get_table(name)
        if table is None:
            print("Creating table named {}...".format(name))
            table = self._sql_table_from_columns(meta, name, columns)
            meta.create_all(self._get_db_engine(), tables=[table])

    def insert_into(self, table_name: str, objects: List[Dict[str, Any]]) -> bool:
        if objects:
            db_connection = self._get_db_engine().connect()
            try:
                table = self._get_table(table_name)
                if table is None:
                    raise Exception("There's not table named {} in {}".format(table_name, self.db_url))
                db_connection.execute(table.insert(), objects)
                db_connection.close()
                return True
            except Exception as e:
                print("Failure on inserting {} objects: {} (objects: {})".format(len(objects), e, objects))
                db_connection.close()
                return False
        else:
            return True

    def drop_table(self, name: str) -> None:
        the_table = self._get_table(name)
        if the_table is not None:
            the_table.drop(bind=self._get_db_engine())

    def _get_table(self, name: str) -> Optional[Table]:
        meta = MetaData()
        meta.reflect(bind=self._get_db_engine())
        try:
            return meta.tables[name]
        except KeyError as e:
            print("Could not find DB table named {}: {}".format(name, e))
            return None

    def count(self, table_name: str) -> int:
        db_connection = self._get_db_engine().connect()
        try:
            table = self._get_table(table_name)
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
            print("Connecting to DB with connection {}".format(self.db_url))
            self._db_engine = create_engine(self.db_url, echo=False, poolclass=NullPool)
        return self._db_engine

    def _sql_table_from_columns(self, sql_metadata: MetaData, table_name: str, columns: List[ConfColumn]) -> Table:
        id_column = Column("id", Integer, primary_key=True, autoincrement=True)
        sql_columns = [Column(c.name, c.sql_type, nullable=c.nullable, index=c.indexed, unique=c.unique)
                       for c in columns]
        return Table(table_name, sql_metadata, id_column, *sql_columns)
