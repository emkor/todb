import argparse
import json
from datetime import datetime
from os import path

from todb.config import ToDbConfig
from todb.util import seconds_between


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Import CSV/TSV files into any SQL DB system')
    parser.add_argument('config', type=str, help='Use given configuration file')
    parser.add_argument('input', type=str, help='A CSV/TSV file to import into DB')
    return parser.parse_args()


def cli_main() -> None:
    args = parse_args()
    main(args)


def main(args: argparse.Namespace) -> None:
    start_time = datetime.utcnow()
    print("Running with: {}!".format(args))
    json_config = json.loads(args.config) if args.config is not None and path.exists(args.config) else {}
    config = ToDbConfig(json_config)
    print("Parsed config to: {}".format(config))
    print("Done in {:10.3f}s!".format(seconds_between(start_time)))


if __name__ == "__main__":
    cli_main()
