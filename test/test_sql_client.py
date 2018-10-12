import unittest
from datetime import date, time, datetime

from test.test_db_utils import setup_db_repository_test_class, get_test_db_engine, TEST_SQL_DB_URL
from todb.data_model import ConfColumn
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
        self.objects = [
            {
                "test_string": "Some text",
                "test_int": -120,
                "test_bigint": 65000120960,
                "test_float": -4.60,
                "test_bool": True,
                "test_date": date(2016, 4, 21),
                "test_time": time(10, 45, 21, 0),
                "test_datetime": datetime(2016, 4, 21, 10, 45, 21)
            }
        ]
        self.client = SqlClient(TEST_SQL_DB_URL, get_test_db_engine(debug=True))
        self.table_name = "test_table"

    def tearDown(self):
        self.client.drop_table(self.table_name)

    def test_should_create_and_drop_tables(self):
        self.client.init_table(self.table_name, self.columns)
        actual_table = self.client.get_table(self.table_name)
        self.assertIsNotNone(actual_table)
        self.assertEqual(len(actual_table.columns), len(self.columns) + 1)

    def test_should_create_table_and_insert_data(self):
        self.client.init_table(self.table_name, self.columns)
        actual_table = self.client.get_table(self.table_name)
        self.client.insert_into(actual_table, self.objects)
        row_count = self.client.count(actual_table)
        self.assertEqual(row_count, len(self.objects))
