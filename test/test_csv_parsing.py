import unittest

from todb.config import ToDbConfig
from todb.parsing import DsvParser
from todb.util import rel_project_path_to_absolute


class DsvParsingTest(unittest.TestCase):
    def test_should_parse_example_csv_file(self):
        abs_csv_path = rel_project_path_to_absolute("resources/example.csv")
        config = ToDbConfig({"parsing": {"buffer_size_kB": 1}})
        parser = DsvParser(config)

        all_lines = []
        for lines in parser.read_rows_in_chunks(abs_csv_path):
            all_lines.extend(lines)
        self.assertIn("Better Person,Zakochany Czlowiek,Zakochany Czlowiek,29 Aug 2018 17:55", all_lines)
        self.assertIn("Niechęć,[self-titled],Metanol,30 Aug 2018 07:41", all_lines)
        self.assertIn("Hope Sandoval & The Warm Inventions,Until the Hunter,Into The Trees,30 Aug 2018 12:16",
                      all_lines)
        self.assertIn("Artist,Album,Title,Date", all_lines)
        self.assertEqual(len(all_lines), 26)