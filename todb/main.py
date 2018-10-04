import argparse
import json
from datetime import datetime
from os import path

from todb.config import ToDbConfig
from todb.data_types import parse_model_file
from todb.util import seconds_between


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Import CSV/TSV files into any SQL DB system')
    parser.add_argument('config', type=str, help='File containing parsing config')
    parser.add_argument('model', type=str, help='File containing model of CSV data')
    parser.add_argument('input', type=str, help='A CSV/TSV file to import into DB')
    return parser.parse_args()


def cli_main() -> None:
    args = parse_args()
    main(args)


def main(args: argparse.Namespace) -> None:
    start_time = datetime.utcnow()
    print("Running with: {}!".format(args))

    config = ToDbConfig.from_file(args.config) if args.config is not None and path.exists(args.config) else ToDbConfig({})
    print("Parsed config to: {}".format(config))

    columns = parse_model_file(args.model) if args.model is not None and path.exists(args.model) else []
    print("Parsed model columns: {}".format(columns))

    print("Done in {:2.3f}s!".format(seconds_between(start_time)))


if __name__ == "__main__":
    cli_main()
