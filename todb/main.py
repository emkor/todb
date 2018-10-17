import argparse
import logging
import traceback
from typing import Tuple

from todb.logger import setup_logger, get_logger
from todb.params import InputParams
from todb.util import seconds_between

from datetime import datetime
from os import path

from todb.data_model import parse_model_file
from todb.parallel_executor import ParallelExecutor

EXIT_CODE_OK = 0
EXIT_CODE_USER_ERROR = 1
EXIT_CODE_FAILURE = 2


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Import CSV/TSV files into any SQL DB system')
    parser.add_argument('model', type=str, help='JSON file containing CSV data model')
    parser.add_argument('input', type=str, help='CSV/TSV file to import into DB')
    parser.add_argument('sql_db', type=str,
                        help='A sqlalchemy-compatible database URL; see https://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls')
    parser.add_argument('--failures', type=str,
                        help='A CSV/TSV file that will contain rows failing to insert into DB; defaults to input file + _failed suffix')
    parser.add_argument('--table', type=str,
                        help='Table name to insert data to; by default, table name will be generated from input file name and current time')
    parser.add_argument('--proc', type=int,
                        help='Number of processes used to parse rows and insert data into DB; default: 2')
    parser.add_argument('--chunk', type=int,
                        help='Size (in kB) of chunk of data that is read from input file and inserted into DB in batched SQL statement; default: 16')
    parser.add_argument('--debug', action='store_true', help='Increases logging verbosity')
    return parser.parse_args()


def _to_db(params: InputParams) -> Tuple[int, int]:
    logger = get_logger()
    columns, pkey, file_config = parse_model_file(params.model_path)
    logger.debug("Parsed model columns: {}".format(columns))

    executor = ParallelExecutor(params, file_config, columns, pkey, params.table_name, params.fail_output_path)
    csv_rows, db_rows = executor.start(params.input_path)
    return csv_rows, db_rows


def cli_main() -> None:
    args = _parse_args()
    main(args)


def main(args: argparse.Namespace) -> None:
    setup_logger(level=logging.DEBUG if args.debug else logging.INFO)
    logger = get_logger()
    try:
        params = InputParams.from_args(args)
        try:
            start_time = datetime.utcnow()
            csv_rows, db_rows = _to_db(params)
            took_seconds = seconds_between(start_time)
            velocity_kBps, velocity_rows_sec = (path.getsize(args.input) / 1000) / took_seconds, csv_rows / took_seconds
            success_percentage = db_rows * 100 / csv_rows
            logger.info(
                "Inserted {} / {} ({:.1f}%) rows in {:.2f}s ({:.1f} kB/s, {:.1f} rows/s)".format(db_rows, csv_rows,
                                                                                                   success_percentage,
                                                                                                   took_seconds,
                                                                                                   velocity_kBps,
                                                                                                   velocity_rows_sec))
            exit(EXIT_CODE_OK)
        except Exception as e:
            logger.error("Error: {} ()".format(e))
            traceback.print_exc()
            exit(EXIT_CODE_FAILURE)
    except Exception as e:
        logger.error("Provided arguments were not correct: {} (args: {})".format(e, args))
        exit(EXIT_CODE_USER_ERROR)


if __name__ == "__main__":
    cli_main()
