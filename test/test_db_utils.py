from typing import Type
from unittest import TestCase

from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.pool import StaticPool

TEST_SQL_DB_URL = 'sqlite:///'


def setup_db_repository_test_class(cls: Type[TestCase]) -> None:
    cls.db_file_name = "{}_sqlite.db".format(cls.__name__)  # type: ignore
    cls.engine = get_test_db_engine(debug=True)  # type: ignore


def get_test_db_engine(debug: bool = False) -> Engine:
    """Snippet for in-memory DB: http://www.sameratiani.com/2013/09/17/flask-unittests-with-in-memory-sqlite.html"""
    return create_engine(TEST_SQL_DB_URL, echo=debug, poolclass=StaticPool)
