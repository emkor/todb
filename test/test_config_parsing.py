import json
import unittest

from todb.config import ToDbConfig, DEFAULT_DB_URL, DEFAULT_PARSING_BUFFER_SIZE_kB, DEFAULT_PARSING_CONCURRENCY, \
    InputFileConfig

TO_DB_CONFIG = """
{
  "parsing": {
    "chunk_size_kB": 128,
    "processes": 2
  },
  "db_url": "postgresql://user:secret@localhost/default"
}
"""

INPUT_FILE_CONFIG = """
{
    "encoding": "ascii",
    "has_header": false,
    "row_delimiter": "\\t",
    "cell_delimiter": ";"
  }
"""


class ToDbConfigParsingTest(unittest.TestCase):
    def test_should_return_default_config_on_empty_input(self):
        default_todb_config = ToDbConfig(conf_dict={})
        self.assertEqual(default_todb_config.db_url(), DEFAULT_DB_URL)
        self.assertEqual(default_todb_config.chunk_size_kB(), DEFAULT_PARSING_BUFFER_SIZE_kB)
        self.assertEqual(default_todb_config.processes(), DEFAULT_PARSING_CONCURRENCY)

    def test_should_return_custom_config_on_custom_input(self):
        custom_todb_config = ToDbConfig(conf_dict=json.loads(TO_DB_CONFIG))
        self.assertEqual(custom_todb_config.db_url(), "postgresql://user:secret@localhost/default")
        self.assertEqual(custom_todb_config.chunk_size_kB(), 128)
        self.assertEqual(custom_todb_config.processes(), 2)


class InputFileConfigParsingTest(unittest.TestCase):
    def test_should_return_default_config_on_empty_input(self):
        default_config = InputFileConfig(conf_dict={})
        self.assertEqual(default_config.file_encoding(), "utf-8")
        self.assertEqual(default_config.has_header(), True)
        self.assertEqual(default_config.row_delimiter(), "\n")
        self.assertEqual(default_config.cell_delimiter(), ",")

    def test_should_return_custom_config_on_custom_input(self):
        custom_config = InputFileConfig(conf_dict=json.loads(INPUT_FILE_CONFIG))
        self.assertEqual(custom_config.file_encoding(), "ascii")
        self.assertEqual(custom_config.has_header(), False)
        self.assertEqual(custom_config.row_delimiter(), "\t")
        self.assertEqual(custom_config.cell_delimiter(), ";")
