import json
import unittest

from todb.data_model import InputFileConfig

INPUT_FILE_CONFIG = """
{
    "encoding": "ascii",
    "has_header_row": false,
    "row_delimiter": "\\t",
    "cell_delimiter": ";"
}
"""


class InputFileConfigParsingTest(unittest.TestCase):
    def test_should_return_default_config_on_empty_input(self):
        default_config = InputFileConfig(conf_dict={})
        self.assertEqual(default_config.file_encoding(), "utf-8")
        self.assertEqual(default_config.has_header_row(), True)
        self.assertEqual(default_config.row_delimiter(), "\n")
        self.assertEqual(default_config.cell_delimiter(), ",")

    def test_should_return_custom_config_on_custom_input(self):
        custom_config = InputFileConfig(conf_dict=json.loads(INPUT_FILE_CONFIG))
        self.assertEqual(custom_config.file_encoding(), "ascii")
        self.assertEqual(custom_config.has_header_row(), False)
        self.assertEqual(custom_config.row_delimiter(), "\t")
        self.assertEqual(custom_config.cell_delimiter(), ";")
