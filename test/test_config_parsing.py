import json
import unittest

from todb.config import ToDbConfig, DEFAULT_DB_HOST, DEFAULT_DB_PORT, DEFAULT_DB_USER, DEFAULT_DB_PASSWORD, \
    DEFAULT_DB_DATABASE, DEFAULT_FILE_ENCODING, DEFAULT_CELL_DELIMITER, DEFAULT_ROW_DELIMITER, DEFAULT_HAS_HEADER, \
    DEFAULT_PARSING_BUFFER_SIZE_kB, DEFAULT_PARSING_CONCURRENCY

CUSTOM_JSON = """
{
  "file": {
    "encoding": "ascii",
    "has_header": false,
    "row_delimiter": ";",
    "cell_delimiter": "\\t"
  },
  "parsing": {
    "buffer_size_kB": 16,
    "concurrency": 2
  },
  "db": {
    "type": "pgsql",
    "host": "pgsql.local",
    "port": 5432,
    "user": "root",
    "password": "secret",
    "database": "myDb"
  }
}
"""


class ToDbConfigParsingTest(unittest.TestCase):
    def test_should_return_default_config_on_empty_input(self):
        default_config = ToDbConfig(conf_dict={})
        self.assertEqual(default_config.db_host(), DEFAULT_DB_HOST)
        self.assertEqual(default_config.db_port(), DEFAULT_DB_PORT)
        self.assertEqual(default_config.db_user(), DEFAULT_DB_USER)
        self.assertEqual(default_config.db_password(), DEFAULT_DB_PASSWORD)
        self.assertEqual(default_config.db_database(), DEFAULT_DB_DATABASE)
        self.assertEqual(default_config.file_encoding(), DEFAULT_FILE_ENCODING)
        self.assertEqual(default_config.cell_delimiter(), DEFAULT_CELL_DELIMITER)
        self.assertEqual(default_config.row_delimiter(), DEFAULT_ROW_DELIMITER)
        self.assertEqual(default_config.has_header(), DEFAULT_HAS_HEADER)
        self.assertEqual(default_config.parsing_buffer_size_kB(), DEFAULT_PARSING_BUFFER_SIZE_kB)
        self.assertEqual(default_config.parsing_concurrency(), DEFAULT_PARSING_CONCURRENCY)

    def test_should_return_custom_config_on_custom_input(self):
        custom_config = ToDbConfig(conf_dict=json.loads(CUSTOM_JSON))
        self.assertEqual(custom_config.db_host(), "pgsql.local")
        self.assertEqual(custom_config.db_port(), 5432)
        self.assertEqual(custom_config.db_user(), "root")
        self.assertEqual(custom_config.db_password(), "secret")
        self.assertEqual(custom_config.db_database(), "myDb")
        self.assertEqual(custom_config.file_encoding(), "ascii")
        self.assertEqual(custom_config.cell_delimiter(), "\t")
        self.assertEqual(custom_config.row_delimiter(), ";")
        self.assertEqual(custom_config.has_header(), False)
        self.assertEqual(custom_config.parsing_buffer_size_kB(), 16)
        self.assertEqual(custom_config.parsing_concurrency(), 2)
