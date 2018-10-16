import unittest

from test.test_db_utils import setup_db_repository_test_class, get_test_db_engine, TEST_SQL_DB_URL
from todb.data_model import ConfColumn, PrimaryKeyConf, PKEY_AUTOINC
from todb.entity_builder import EntityBuilder
from todb.sql_client import SqlClient


class SqlClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_db_repository_test_class(cls)

    def setUp(self):
        self.columns = [ConfColumn("test_string", 0, "string",
                                   nullable=True, indexed=True, unique=True),
                        ConfColumn("test_int", 1, "int",
                                   nullable=False, indexed=False, unique=False),
                        ConfColumn("test_bigint", 2, "bigint",
                                   nullable=True, indexed=False, unique=False),
                        ConfColumn("test_float", 3, "float",
                                   nullable=True, indexed=False, unique=False),
                        ConfColumn("test_bool", 4, "bool",
                                   nullable=False, indexed=True, unique=False),
                        ConfColumn("test_date", 5, "date",
                                   nullable=True, indexed=False, unique=False),
                        ConfColumn("test_time", 6, "time",
                                   nullable=True, indexed=False, unique=False),
                        ConfColumn("test_datetime", 7, "datetime",
                                   nullable=False, indexed=True, unique=False)]
        self.primary_key = PrimaryKeyConf(mode=PKEY_AUTOINC, columns=[])
        self.rows = [[
            "Some text",
            "-120",
            "65000120960",
            "-4.60",
            "true",
            "2016/04/21",
            "10:45:21",
            "2016-04-21 10:45:21",
        ]]
        self.entity_builder = EntityBuilder(self.columns)
        self.client = SqlClient(TEST_SQL_DB_URL, self.entity_builder, get_test_db_engine(debug=True))
        self.table_name = "test_table"

    def tearDown(self):
        self.client.drop_table(self.table_name)

    def test_should_create_and_drop_tables(self):
        self.client.init_table(self.table_name, self.columns, self.primary_key)
        actual_table = self.client._get_table(self.table_name)
        self.assertIsNotNone(actual_table)
        self.assertEqual(len(actual_table.columns), len(self.columns) + 1)

    def test_should_create_table_and_insert_data(self):
        self.client.init_table(self.table_name, self.columns, self.primary_key)
        self.client.insert_into(self.table_name, self.rows)
        row_count = self.client.count(self.table_name)
        self.assertEqual(row_count, len(self.rows))
