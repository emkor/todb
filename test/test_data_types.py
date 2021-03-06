import unittest

from todb.data_model import parse_model_file, ConfColumn, PrimaryKeyConf, PKEY_COLS, handle_lat_lon, handle_float
from todb.util import proj_path_to_abs

LAT_LON_DELTA = 0.0001
FLOAT_DELTA = 0.00001


class DataModelParsingTest(unittest.TestCase):
    def test_should_parse_model_file(self):
        abs_csv_path = proj_path_to_abs("resources/example_model.json")
        model_columns, pkey_config, file_config = parse_model_file(abs_csv_path)
        self.assertEqual(pkey_config, PrimaryKeyConf(PKEY_COLS, ["timestamp", "artist", "title"]))
        self.assertEqual(len(model_columns), 3)
        self.assertIn(ConfColumn(name="artist", col_index=0, conf_type="string",
                                 nullable=False, indexed=True, unique=False), model_columns)

    def test_should_parse_lat_lon_strings(self):
        coord_1 = "20-55-70.010N"
        coord_1_expected = 20.9361138889
        coord_2 = "32-11-50.000W"
        coord_1_actual = handle_lat_lon(coord_1)
        coord_2_expected = -32.1972222222
        coord_2_actual = handle_lat_lon(coord_2)
        self.assertAlmostEqual(coord_1_actual, coord_1_expected, delta=LAT_LON_DELTA)
        self.assertAlmostEqual(coord_2_actual, coord_2_expected, delta=LAT_LON_DELTA)

    def test_should_parse_lat_lon_canonical_strings(self):
        coord_1 = "20°55'70.010""N"
        coord_1_expected = 20.9361138889
        coord_2 = "32°11'50.000""W"
        coord_1_actual = handle_lat_lon(coord_1)
        coord_2_expected = -32.1972222222
        coord_2_actual = handle_lat_lon(coord_2)
        self.assertAlmostEqual(coord_1_actual, coord_1_expected, delta=LAT_LON_DELTA)
        self.assertAlmostEqual(coord_2_actual, coord_2_expected, delta=LAT_LON_DELTA)

    def test_should_parse_float_in_comma_notation(self):
        float_with_commas = "7,562.2"
        float_with_commas_inverted = "7.562,2"
        float_with_comma_as_separator = "7562,2"
        float_with_dot_as_separator = "7562.2"
        expected_float = 7562.2
        self.assertAlmostEqual(handle_float(float_with_commas), expected_float, delta=FLOAT_DELTA)
        self.assertAlmostEqual(handle_float(float_with_commas_inverted), expected_float, delta=FLOAT_DELTA)
        self.assertAlmostEqual(handle_float(float_with_comma_as_separator), expected_float, delta=FLOAT_DELTA)
        self.assertAlmostEqual(handle_float(float_with_dot_as_separator), expected_float, delta=FLOAT_DELTA)
