import unittest

from todb.data_types import parse_model_file, ConfColumn
from todb.util import proj_path_to_abs


class DataModelParsingTest(unittest.TestCase):
    def test_should_parse_model_file(self):
        abs_csv_path = proj_path_to_abs("resources/model_example.json")
        model_columns = parse_model_file(abs_csv_path)
        self.assertEqual(len(model_columns), 3)
        self.assertIn(ConfColumn(name="artist", col_index=0, conf_type="string",
                                 nullable=False, index=True, unique=False), model_columns)
