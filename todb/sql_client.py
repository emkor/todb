from typing import List, Dict, Any, Optional, Tuple

from sqlalchemy import MetaData, Column, Table, select, func, Integer
from sqlalchemy.engine import Engine, create_engine, Connection
from sqlalchemy.pool import NullPool

from todb.data_model import ConfColumn, PrimaryKeyConf, PKEY_AUTOINC
from todb.db_client import DbClient
from todb.entity_builder import EntityBuilder


class SqlClient(DbClient):
    def __init__(self, db_url: str, entity_builder: EntityBuilder, db_engine: Optional[Engine] = None) -> None:
        self.db_url = db_url
        self.entity_builder = entity_builder
        self._db_engine = db_engine

    def init_table(self, name: str, columns: List[ConfColumn], pkey: PrimaryKeyConf) -> None:
        meta = MetaData()
        table = self._get_table(name)
        if table is None:
            print("Creating table named {}...".format(name))
            table = self._sql_table_from_columns(meta, name, columns, pkey)
            meta.create_all(self._get_db_engine(), tables=[table])

    def drop_table(self, name: str) -> None:
        the_table = self._get_table(name)
        if the_table is not None:
            the_table.drop(bind=self._get_db_engine())

    def insert_into(self, table_name: str, rows: List[List[str]]) -> List[List[str]]:
        mass_insert_successful, failed_rows = self._insert_all(table_name, rows)
        if mass_insert_successful:
            return failed_rows
        else:
            failed_rows = self._insert_one_by_one(table_name, rows)
        return failed_rows

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

    def _insert_all(self, table_name: str, rows: List[List[str]]) -> Tuple[bool, List[List[str]]]:
        list_of_model_dicts, failed_rows = self._build_entities_from_rows(rows)
        if list_of_model_dicts:
            db_connection = None
            try:
                db_connection = self._insert_entities(list_of_model_dicts, table_name)
                return True, failed_rows
            except Exception as e:
                print("Failed to insert {} objects at once: {}".format(len(list_of_model_dicts), e))
                if db_connection is not None and not db_connection.closed:
                    db_connection.close()
                return False, failed_rows
        else:
            return True, failed_rows

    def _insert_one_by_one(self, table_name: str, rows: List[List[str]]) -> List[List[str]]:
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
        db_connection = self._get_db_engine().connect()
        table = self._get_table(table_name)
        if table is None:
            raise Exception("There's not table named {} in {}".format(table_name, self.db_url))
        db_connection.execute(table.insert(), list_of_model_dicts)
        db_connection.close()
        return db_connection

    def _get_table(self, name: str) -> Optional[Table]:
        meta = MetaData()
        meta.reflect(bind=self._get_db_engine())
        try:
            return meta.tables[name]
        except KeyError as e:
            print("Could not find DB table named {}: {}".format(name, e))
            return None

    def _get_db_engine(self) -> Engine:
        if self._db_engine is None:
            print("Connecting to DB with connection {}".format(self.db_url))
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
