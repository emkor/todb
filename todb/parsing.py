from typing import Iterator, List

from todb.data_model import InputFileConfig
from todb.logger import get_logger


class CsvParser(object):
    def __init__(self, input_file_config: InputFileConfig, chunk_size_kB: int) -> None:
        self.chunk_size_kB = chunk_size_kB
        self.input_file_config = input_file_config
        self.logger = get_logger()

    def read_rows_in_chunks(self, file_path: str) -> Iterator[List[List[str]]]:
        buffer_size_bytes = round(self.chunk_size_kB * 1000, ndigits=None)
        cached_last_line = ""
        has_header_row = self.input_file_config.has_header_row()
        with open(file_path, "rb") as input_file:
            while True:
                data = input_file.read(buffer_size_bytes)
                if not data:
                    break
                try:
                    one_line = cached_last_line + data.decode(self.input_file_config.file_encoding())
                    rows = one_line.split(self.input_file_config.row_delimiter())
                    cached_last_line = rows.pop(-1)  # last line may not be complete up to row delimiter!
                    if has_header_row:
                        rows.pop(0)  # remove first row as header and don't remove it again
                        has_header_row = False
                    cells_in_rows = [r.split(self.input_file_config.cell_delimiter()) for r in rows]
                    yield cells_in_rows
                except Exception as e:
                    self.logger.error("Error on parsing CSV: {}".format(e))
                    cached_last_line = ""
                    yield []
            # don't forget last cached line
            yield [cached_last_line.split(self.input_file_config.cell_delimiter())] if cached_last_line else []
