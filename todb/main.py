import argparse
from datetime import datetime
from os import path

from todb.to_db import to_db
from todb.util import seconds_between


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Import CSV/TSV files into any SQL DB system')
    parser.add_argument('config', type=str, help='File containing parsing config')
    parser.add_argument('model', type=str, help='File containing model of CSV data')
    parser.add_argument('input', type=str, help='A CSV/TSV file to import into DB')
    return parser.parse_args()


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
            to_db(config_file_name=args.config, model_file_name=args.model, input_file_name=args.input)
            took_seconds = seconds_between(start_time)
            velocity_kBps = (input_file_size / 1000) / took_seconds
            print("Done in {:2.3f}s ({:3.1f} kB/s)!".format(took_seconds, velocity_kBps))
        except Exception as e:
            print("Error: {}".format(e))
            exit(2)
    else:
        print("Did not provide config, model or input or those files does not exist.")
        exit(1)


if __name__ == "__main__":
    cli_main()
    exit(0)
