from typing import List, Dict, Any, Optional, Tuple

from sqlalchemy import MetaData, Column, Table, select, func, Integer
from sqlalchemy.engine import Engine, create_engine, Connection
from sqlalchemy.pool import NullPool

from todb.data_model import ConfColumn, PrimaryKeyConf, PKEY_AUTOINC
from todb.db_client import DbClient
from todb.entity_builder import EntityBuilder
from todb.logger import get_logger

INSERT_ONE_BY_ONE_THRESHOLD = 8


class SqlClient(DbClient):
    def __init__(self, db_url: str, entity_builder: EntityBuilder, db_engine: Optional[Engine] = None,
                 ca_file: Optional[str] = None) -> None:
        self.db_url = db_url
        self.ca_file = ca_file
        self.entity_builder = entity_builder
        self._db_engine = db_engine
        self.logger = get_logger()
        self._conn = None  # type: Optional[Connection]

    def init_table(self, name: str, columns: List[ConfColumn], pkey: PrimaryKeyConf) -> None:
        meta = MetaData()
        table = self._get_table(name)
        if table is None:
            self.logger.info("Creating table named {}...".format(name))
            table = self._sql_table_from_columns(meta, name, columns, pkey)
            meta.create_all(self._get_db_engine(), tables=[table])

    def drop_table(self, name: str) -> None:
        the_table = self._get_table(name)
        if the_table is not None:
            the_table.drop(bind=self._get_db_engine())

    def count(self, table_name: str) -> int:
        try:
            table = self._get_table(table_name)
            count = self._get_connection().scalar(
                select([func.count()]).select_from(table)
            )
        except Exception as e:
            self.logger.error("Could not count: {}".format(e))
            count = 0
        return count

    def insert_in_batch(self, table_name: str, rows: List[List[str]]) -> Tuple[bool, List[List[str]]]:
        list_of_model_dicts, failed_rows = self._build_entities_from_rows(rows)
        if list_of_model_dicts:
            try:
                self._insert_entities(list_of_model_dicts, table_name)
                return True, failed_rows
            except Exception as e:
                self.logger.debug("Failed to insert {} objects in batch: {}".format(len(list_of_model_dicts), e))
                return False, failed_rows
        else:
            return True, failed_rows

    def insert_one_by_one(self, table_name: str, rows: List[List[str]]) -> List[List[str]]:
        all_failed_rows = []
        for row in rows:
            list_of_entities, failed_rows = self._build_entities_from_rows([row])
            all_failed_rows.extend(failed_rows)
            if list_of_entities:
                try:
                    self._insert_entities(list_of_entities, table_name)
                except Exception:
                    all_failed_rows.append(row)
        return all_failed_rows

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()

    def _build_entities_from_rows(self, rows: List[List[str]]) -> Tuple[List[Dict[str, Any]], List[List[str]]]:
        list_of_model_dicts = []
        failed_rows = []
        for row_cells in rows:
            entity = self.entity_builder.to_entity(row_cells)
            if entity is not None:
                list_of_model_dicts.append(entity)
            else:
                failed_rows.append(row_cells)
        return list_of_model_dicts, failed_rows

    def _insert_entities(self, list_of_model_dicts: List[Dict[str, Any]], table_name: str) -> Connection:
        db_connection = self._get_connection()
        table = self._get_table(table_name)
        if table is None:
            raise Exception("There's not table named {} in {}".format(table_name, self.db_url))
        db_connection.execute(table.insert(), list_of_model_dicts)
        return db_connection

    def _get_table(self, name: str) -> Optional[Table]:
        meta = MetaData()
        meta.reflect(bind=self._get_db_engine())
        try:
            return meta.tables[name]
        except KeyError as e:
            self.logger.debug("Could not find DB table named {}: {}".format(name, e))
            return None

    def _get_connection(self) -> Connection:
        if self._conn is None or self._conn.closed:
            self._conn = self._get_db_engine().connect()
        return self._conn

    def _get_db_engine(self) -> Engine:
        if self._db_engine is None:
            self.logger.debug("Connecting to DB with connection {}".format(self.db_url))
            if self.ca_file is not None:
                ssl_args = {'ssl': {'cert': self.ca_file}}
                self._db_engine = create_engine(self.db_url, echo=False, poolclass=NullPool, connect_args=ssl_args)
            else:
                self._db_engine = create_engine(self.db_url, echo=False, poolclass=NullPool)
        return self._db_engine

    def _sql_table_from_columns(self, sql_metadata: MetaData, table_name: str, columns: List[ConfColumn],
                                pkey: PrimaryKeyConf) -> Table:
        sql_columns = {c.name: Column(c.name, c.sql_type, primary_key=c.name in pkey.columns,
                                      nullable=c.nullable, index=c.indexed, unique=c.unique)
                       for c in columns}
        if pkey.mode == PKEY_AUTOINC:
            id_column = Column("ID", Integer, primary_key=True, autoincrement=True)
            sql_columns.update({"id": id_column})
        return Table(table_name, sql_metadata, *sql_columns.values())
