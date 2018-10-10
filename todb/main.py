import argparse
from typing import Optional

from todb.util import seconds_between

from datetime import datetime
from os import path

from todb.todb_config import config_from_file
from todb.data_model import parse_model_file
from todb.parallel_executor import ParallelExecutor

EXIT_CODE_OK = 0
EXIT_CODE_USER_ERROR = 1
EXIT_CODE_FAILURE = 2


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Import CSV/TSV files into any SQL DB system')
    parser.add_argument('config', type=str, help='File containing parsing config')
    parser.add_argument('model', type=str, help='File containing model of CSV data')
    parser.add_argument('input', type=str, help='A CSV/TSV file to import into DB')
    parser.add_argument('--table', type=str, help='Table name to insert data to')
    return parser.parse_args()


def _to_db(config_file_name: str, model_file_name: str, input_file_name: str, table_name: Optional[str] = None) -> None:
    todb_config = config_from_file(config_file_name)
    print("Parsed config to: {}".format(todb_config))

    columns, file_config = parse_model_file(model_file_name)
    print("Parsed model columns: {}".format(columns))

    current_time = datetime.utcnow().replace(microsecond=0).time().isoformat()
    table_name = table_name or "todb_{}_{}".format(path.basename(input_file_name)[:32], current_time)
    executor = ParallelExecutor(todb_config, file_config, columns, table_name)
    csv_rows, db_rows = executor.start(input_file_name)
    print("Inserted {} rows out of {} available ({}%)".format(db_rows, csv_rows, round(db_rows * 100 / csv_rows)))


def cli_main() -> None:
    args = _parse_args()
    main(args)


def main(args: argparse.Namespace) -> None:
    if args.config is not None and path.exists(args.config) \
            and args.model is not None and path.exists(args.model) \
            and args.input is not None and path.exists(args.input):
        try:
            start_time = datetime.utcnow()
            print("Running with: {}!".format(args))
            input_file_size = path.getsize(args.input)
            _to_db(config_file_name=args.config, model_file_name=args.model,
                   input_file_name=args.input, table_name=args.table)
            took_seconds = seconds_between(start_time)
            velocity_kBps = (input_file_size / 1000) / took_seconds
            print("Done in {:2.3f}s ({:3.1f} kB/s)!".format(took_seconds, velocity_kBps))
            exit(EXIT_CODE_OK)
        except Exception as e:
            print("Error: {}".format(e))
            exit(EXIT_CODE_FAILURE)
    else:
        print("Did not provide config, model or input or those files does not exist.")
        exit(EXIT_CODE_USER_ERROR)


if __name__ == "__main__":
    cli_main()
