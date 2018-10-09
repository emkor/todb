import unittest

from todb.config import ToDbConfig, InputFileConfig
from todb.parsing import CsvParser
from todb.util import proj_path_to_abs


class CsvParsingTest(unittest.TestCase):
    def test_should_parse_example_csv_file(self):
        abs_csv_path = proj_path_to_abs("resources/example_input.csv")
        todb_config = ToDbConfig({"parsing": {"chunk_size_kB": 1}})
        in_file_config = InputFileConfig({"has_header": True})
        parser = CsvParser(in_file_config, todb_config)

        all_lines = []
        for lines in parser.read_rows_in_chunks(abs_csv_path):
            all_lines.extend(lines)
        self.assertIn(['Better Person', 'Zakochany Czlowiek', 'Zakochany Czlowiek', '29 Aug 2018 17:55'], all_lines)
        self.assertIn(['Niechęć', '[self-titled]', 'Rajza', '30 Aug 2018 07:58'], all_lines)
        self.assertIn(['Hope Sandoval & The Warm Inventions', 'Until the Hunter',
                       'Into The Trees', '30 Aug 2018 12:16'],
                      all_lines)
        self.assertNotIn(["Artist", "Album", "Title", "Date"], all_lines)
        self.assertEqual(len(all_lines), 25)
