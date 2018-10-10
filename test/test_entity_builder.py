import unittest
from datetime import datetime, date, time

from todb.data_model import ConfColumn
from todb.entity_builder import EntityBuilder


class EntityBuilderTest(unittest.TestCase):
    def setUp(self):
        self.columns = [
            ConfColumn("test_string", 0, "string",
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
                       nullable=False, indexed=True, unique=False)
        ]

    def test_should_build_entity_with_each_type(self):
        row = [
            "Some text",
            "-120",
            "65000120960",
            "-4.60",
            "true",
            "2016/04/21",
            "10:45:21",
            "2016-04-21 10:45:21",
        ]
        builder = EntityBuilder(self.columns)
        actual_entity = builder.to_entity(row)
        expected_entity = {
            "test_string": "Some text",
            "test_int": -120,
            "test_bigint": 65000120960,
            "test_float": -4.60,
            "test_bool": True,
            "test_date": date(2016, 4, 21),
            "test_time": time(10, 45, 21, 0),
            "test_datetime": datetime(2016, 4, 21, 10, 45, 21)
        }
        self.assertEqual(actual_entity, expected_entity)

    def test_should_return_none_on_non_nullable_column(self):
        row = [
            "Some text",
            None,
            "65000120960",
            "-4.60",
            "true",
            "2016/04/21",
            "10:45:21",
            "2016-04-21 10:45:21"
        ]
        builder = EntityBuilder(self.columns)
        actual_entity = builder.to_entity(row)
        self.assertEqual(actual_entity, None)

    def test_should_return_none_on_too_many_columns(self):
        row = [
            "Some text",
            "-120",
            "65000120960",
            "-4.60",
            "true",
            "2016/04/21"
        ]
        builder = EntityBuilder(self.columns)
        actual_entity = builder.to_entity(row)
        self.assertEqual(actual_entity, None)

    def test_should_replace_value_with_none_if_column_nullable_and_value_non_parsable(self):
        row = [
            "Some text",
            "-120",
            "nonParseableAsInt10",
            "nonParseableAsFloat",
            "true",
            "2016/04/21",
            "10:45:21",
            "2016-04-21 10:45:21"
        ]
        builder = EntityBuilder(self.columns)
        actual_entity = builder.to_entity(row)
        expected_entity = {
            "test_string": "Some text",
            "test_int": -120,
            "test_bigint": None,
            "test_float": None,
            "test_bool": True,
            "test_date": date(2016, 4, 21),
            "test_time": time(10, 45, 21, 0),
            "test_datetime": datetime(2016, 4, 21, 10, 45, 21)
        }
        self.assertEqual(actual_entity, expected_entity)
