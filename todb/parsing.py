from typing import Iterator, List

from todb.config import ToDbConfig


class CsvParser(object):
    def __init__(self, todb_config: ToDbConfig) -> None:
        self.todb_config = todb_config

    def read_rows_in_chunks(self, file_path: str) -> Iterator[List[List[str]]]:
        buffer_size_bytes = round(self.todb_config.parsing_buffer_size_kB() * 1000, ndigits=None)
        cached_last_line = ""
        has_header = self.todb_config.has_header()
        with open(file_path, "rb") as input_file:
            while True:
                data = input_file.read(buffer_size_bytes)
                if not data:
                    break
                one_line = cached_last_line + data.decode(self.todb_config.file_encoding())
                rows = one_line.split(self.todb_config.row_delimiter())
                cached_last_line = rows.pop(-1)  # last line may not be complete up to row delimiter!
                if has_header:
                    rows.pop(0)  # remove first row as header and don't remove it again
                    has_header = False
                cells_in_rows = [r.split(self.todb_config.cell_delimiter()) for r in rows]
                yield cells_in_rows
            # don't forget last cached line
            yield [cached_last_line.split(self.todb_config.cell_delimiter())] if cached_last_line else []
