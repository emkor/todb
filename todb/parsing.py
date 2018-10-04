from typing import Iterator, List

from todb.config import ToDbConfig


class DsvParser(object):
    """Delimiter-separated-value parser"""

    def __init__(self, todb_config: ToDbConfig) -> None:
        self.todb_config = todb_config

    def read_rows_in_chunks(self, file_path: str) -> Iterator[List[str]]:
        buffer_size_bytes = round(self.todb_config.parsing_buffer_size_kB() * 1000, ndigits=None)
        cached_last_line = ""
        with open(file_path, "rb") as input_file:
            while True:
                data = input_file.read(buffer_size_bytes)
                if not data:
                    break
                one_line = cached_last_line + data.decode(self.todb_config.file_encoding())
                rows = one_line.split(self.todb_config.row_delimiter())
                cached_last_line = rows.pop(-1)  # last line may not be complete up to row delimiter!
                yield rows
            yield [cached_last_line] if cached_last_line else []  # don't forget last cached line