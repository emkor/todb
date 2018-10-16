import argparse
import traceback

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
    return parser.parse_args()


def _to_db(params: InputParams) -> None:
    columns, pkey, file_config = parse_model_file(params.model_path)
    print("Parsed model columns: {}".format(columns))

    executor = ParallelExecutor(params, file_config, columns, pkey, params.table_name, params.fail_output_path)
    csv_rows, db_rows = executor.start(params.input_path)
    print("Inserted {} rows out of {} available ({:3.1f}%)".format(db_rows, csv_rows, db_rows * 100 / csv_rows))


def cli_main() -> None:
    args = _parse_args()
    main(args)


def main(args: argparse.Namespace) -> None:
    try:
        params = InputParams.from_args(args)
        try:
            start_time = datetime.utcnow()
            print("Running with: {}!".format(args))
            input_file_size = path.getsize(args.input)
            _to_db(params)
            took_seconds = seconds_between(start_time)
            velocity_kBps = (input_file_size / 1000) / took_seconds
            print("Done in {:2.3f}s ({:3.1f} kB/s)!".format(took_seconds, velocity_kBps))
            exit(EXIT_CODE_OK)
        except Exception as e:
            print("Error: {} ()".format(e))
            traceback.print_exc()
            exit(EXIT_CODE_FAILURE)
    except Exception as e:
        print("Provided arguments were not correct: {} (args: {})".format(e, args))
        exit(EXIT_CODE_USER_ERROR)


if __name__ == "__main__":
    cli_main()
