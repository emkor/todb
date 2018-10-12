from argparse import Namespace
from datetime import datetime
from os import path
from typing import Optional

from todb.abstract import Model
from todb.util import limit_or_default

DEFAULT_CHUNK_SIZE_kB = 16
MIN_CHUNK_SIZE_kB = 1
MAX_CHUNK_SIZE_kB = 64000

MIN_PROCESSES = 1
DEFAULT_PROCESSES = 2
MAX_PROCESSES = 128


class InputParams(Model):
    @classmethod
    def from_args(cls, args: Namespace):
        return InputParams(model_path=args.model, input_path=args.input, fail_output_path=args.fail_output,
                           sql_db=args.sql_db, cass_db=args.cass_db, table_name=args.table, processes=args.proc,
                           chunk_size_kB=args.chunk)

    def __init__(self, model_path: str, input_path: str, fail_output_path: str,
                 sql_db: str, cass_db: str, table_name: Optional[str] = None,
                 processes: Optional[int] = None, chunk_size_kB: Optional[int] = None) -> None:
        self.model_path = model_path
        self.input_path = input_path
        self.sql_db = sql_db
        self.cass_db = cass_db
        self.fail_output_path = fail_output_path
        self.table_name = table_name or self._generate_table_name(datetime.utcnow())
        self.chunk_size_kB = limit_or_default(value=chunk_size_kB, default=DEFAULT_CHUNK_SIZE_kB,
                                              lower_bound=MIN_CHUNK_SIZE_kB, upper_bound=MAX_CHUNK_SIZE_kB)
        self.processes = limit_or_default(value=processes, default=DEFAULT_PROCESSES,
                                          lower_bound=MIN_PROCESSES, upper_bound=MAX_PROCESSES)
        self.validate()

    def _generate_table_name(self, date_time: datetime) -> str:
        return "{}_{}".format(path.basename(self.input_path)[:48],
                                   date_time.replace(microsecond=0).time().isoformat()).replace(":", "_").replace(".", "_")

    def validate(self):
        self._validate_path_exists(self.input_path, "Input file path of {} is invalid")
        self._validate_path_exists(self.model_path, "Model file path of {} is invalid")
        if self.fail_output_path is None:
            raise ValueError("Failed-rows file output path of {} is invalid".format(self.fail_output_path))
        if not self.sql_db and not self.cass_db:
            raise ValueError("Did not provide any DB credentials!")

    def _validate_path_exists(self, the_path: str, err_msg_fmt: str) -> None:
        if the_path is None or not path.exists(path.abspath(the_path)):
            raise ValueError(err_msg_fmt.format(the_path))
