from typing import List

from todb.data_model import InputFileConfig


class FailRowHandler(object):
    def __init__(self, input_file_config: InputFileConfig, output_file_path: str) -> None:
        self.input_file_config = input_file_config
        self.output_file_path = output_file_path

    def handle_failed_rows(self, rows: List[List[str]]):
        print("Handling {} unsuccessfully inserted rows...".format(len(rows)))
        try:
            out_string = self.input_file_config.row_delimiter().join(
                [self.input_file_config.cell_delimiter().join(cells)
                 for cells in rows])
            out_bytes = out_string.encode(self.input_file_config.file_encoding())
            with open(self.output_file_path, "w+b") as out_file:
                out_file.write(out_bytes)
        except Exception as e:
            print("Could not handle storing rows back in file {}: {}".format(self.output_file_path, e))
