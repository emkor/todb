from datetime import datetime
from os import path
from typing import List

from todb.data_model import InputFileConfig
from todb.logger import get_logger
from todb.util import seconds_between


class FailRowHandler(object):
    def __init__(self, input_file_config: InputFileConfig, output_file_path: str) -> None:
        self.input_file_config = input_file_config
        self.output_file_path = output_file_path
        self.logger = get_logger()

    def handle_failed_rows(self, rows: List[List[str]]):
        start_time = datetime.utcnow()
        self.logger.debug("Logging {} unsuccessfully inserted rows...".format(len(rows)))
        try:
            out_bytes = self._convert_rows_to_bytes(rows)
            with open(self.output_file_path, "ab") as out_file:
                out_file.write(out_bytes)
            took_seconds = seconds_between(start_time)
            self.logger.info("Logged {} unsuccessfully inserted rows in {:.2f}s".format(len(rows), took_seconds))
        except Exception as e:
            self.logger.error("Could not handle storing rows back in file {}: {}; rows: \n{}".format(path.basename(
                self.output_file_path), e, rows))

    def _convert_rows_to_bytes(self, rows: List[List[str]]) -> bytes:
        out_string = self.input_file_config.row_delimiter().join(
            [self.input_file_config.cell_delimiter().join(cells)
             for cells in rows])
        out_bytes = ("\n" + out_string).encode(self.input_file_config.file_encoding())
        return out_bytes
