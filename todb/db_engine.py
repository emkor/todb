from typing import Optional, List

from sqlalchemy import create_engine, Table, Column, MetaData, BigInteger
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from todb.config import ToDbConfig
from todb.data_types import ConfColumn

_ENGINE = None  # type: Optional[Engine]
_SESSION_MAKER = None  # type: Optional[sessionmaker]


def get_db_engine(todb_config: ToDbConfig) -> Engine:
    global _ENGINE
    if _ENGINE is None:
        db_connection_string = "{}://{}:{}@{}:{}/{}".format(todb_config.db_type(),
                                                            todb_config.db_user(),
                                                            todb_config.db_password(),
                                                            todb_config.db_host(),
                                                            todb_config.db_port(),
                                                            todb_config.db_database())
        print("Connecting to DB with connection {}".format(db_connection_string))
        _ENGINE = create_engine(db_connection_string, echo=True, poolclass=NullPool)
    return _ENGINE


def sql_table_from_columns(sql_metadata: MetaData, table_name: str, columns: List[ConfColumn]) -> Table:
    id_column = Column("id", BigInteger, primary_key=True, autoincrement=True)
    sql_columns = [Column(c.name, c.sql_type, nullable=c.nullable,
                          primary_key=c.is_key(), index=c.indexed, unique=c.unique)
                   for c in columns]
    return Table(table_name, sql_metadata, id_column, *sql_columns)
