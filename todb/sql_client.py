from typing import List, Dict, Any

from sqlalchemy import MetaData, BigInteger, Column, Table
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.pool import NullPool

from todb.config import ToDbConfig
from todb.data_types import ConfColumn


class SqlClient(object):
    def __init__(self, todb_config: ToDbConfig) -> None:
        self.todb_config = todb_config
        self._db_engine = None

    def init_table(self, name: str, columns: List[ConfColumn]) -> Table:
        meta = MetaData()
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

    def get_table(self, name: str) -> Table:
        meta = MetaData()
        meta.reflect(bind=self._get_db_engine())
        return meta.tables[name]

    def _get_db_engine(self) -> Engine:
        if self._db_engine is None:
            db_connection_string = "{}://{}:{}@{}:{}/{}".format(self.todb_config.db_type(),
                                                                self.todb_config.db_user(),
                                                                self.todb_config.db_password(),
                                                                self.todb_config.db_host(),
                                                                self.todb_config.db_port(),
                                                                self.todb_config.db_database())
            print("Connecting to DB with connection {}".format(db_connection_string))
            self._db_engine = create_engine(db_connection_string, echo=False, poolclass=NullPool)
        return self._db_engine

    def _sql_table_from_columns(self, sql_metadata: MetaData, table_name: str, columns: List[ConfColumn]) -> Table:
        id_column = Column("id", BigInteger, primary_key=True, autoincrement=True)
        sql_columns = [Column(c.name, c.sql_type, nullable=c.nullable,
                              primary_key=c.is_key(), index=c.indexed, unique=c.unique)
                       for c in columns]
        return Table(table_name, sql_metadata, id_column, *sql_columns)
