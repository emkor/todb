import unittest
from unittest.mock import Mock, ANY

from todb.data_model import ConfColumn
from todb.entity_builder import EntityBuilder
from todb.importer import Importer
from todb.sql_client import SqlClient


class ImporterTest(unittest.TestCase):
    def setUp(self):
        self.columns = [ConfColumn("test_string", 0, "string", nullable=True, indexed=True, unique=True)]
        self.mock_sql_client = Mock(SqlClient)
        self.entity_builder = EntityBuilder(self.columns)
        self.importer = Importer(self.entity_builder, self.mock_sql_client, "some_table")

    def test_should_call_sql_client_for_insert(self):
        self.importer.parse_and_import([["string_0"], ["string_1"]])
        self.mock_sql_client.insert_into.assert_called_once_with(table=ANY, objects=[{"test_string": "string_0"},
                                                                                     {"test_string": "string_1"}])
