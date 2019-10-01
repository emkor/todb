from typing import Iterator, Optional, List
import io

import time
from functools import wraps
from memory_profiler import memory_usage
import psycopg2.extras


def profile(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
        fn_kwargs_str = ', '.join(f'{k}={v}' for k, v in kwargs.items())
        print(f'\n{fn.__name__}({fn_kwargs_str})')

        # Measure time
        t = time.perf_counter()
        retval = fn(*args, **kwargs)
        elapsed = time.perf_counter() - t
        print(f'Time   {elapsed:0.4}')

        # Measure memory
        mem, retval = memory_usage((fn, args, kwargs), retval=True, timeout=200, interval=1e-7)

        print(f'Memory {max(mem) - min(mem)}')
        return retval

    return inner


class StringIteratorIO(io.TextIOBase):

    def __init__(self, iter: Iterator[str]):
        self._iter = iter
        self._buff = ''

    def readable(self) -> bool:
        return True

    def _read1(self, n: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return ''.join(line)


def read_rows_in_chunks(file_path: str, cell_delimiter: str = ",", row_delimiter: str = "\n",
                        file_enc: str = "utf-8", chunk_size_kB: int = 1.) -> Iterator[List[List[str]]]:
    buffer_size_bytes = round(chunk_size_kB * 1024, ndigits=None)
    cached_last_line = ""
    with open(file_path, "rb") as input_file:
        while True:
            data = input_file.read(buffer_size_bytes)
            if not data:
                break
            try:
                one_line = cached_last_line + data.decode(file_enc)
                rows = one_line.split(row_delimiter)
                cached_last_line = rows.pop(-1)  # last line may not be complete up to row delimiter
                cells_in_rows = [r.split(cell_delimiter) for r in rows]
                yield cells_in_rows
            except Exception as e:
                print("Error on parsing CSV: {}".format(e))
                cached_last_line = ""
                yield []
        yield [cached_last_line.split(cell_delimiter)] if cached_last_line else []  # don't forget last cached line


@profile
def copy_string_iterator(conn, batch_of_rows: Iterator[List[List[str]]], size: int = 8192) -> None:
    beers_string_iterator = StringIteratorIO((
        '|'.join((row[0], row[3], row[2], row[1], row[4], row[5])) + '\n'
        for rows in batch_of_rows for row in rows
    ))
    with conn.cursor() as cursor:
        cursor.copy_from(beers_string_iterator, 'mpk_2019_09_01', sep='|', size=size)


if __name__ == '__main__':
    connection = psycopg2.connect(
        host='localhost',
        port=5432,
        database='mpk',
        user='admin',
        password='secret',
    )
    connection.set_session(autocommit=True)
    csv_rows_generator = read_rows_in_chunks('/home/mat/proj/todb/mpk_data/2019-09-01.csv', cell_delimiter=";",
                                             chunk_size_kB=8)
    print("Starting insertion...")
    copy_string_iterator(connection, csv_rows_generator, size=1024 * 8)
    print("Done!")
