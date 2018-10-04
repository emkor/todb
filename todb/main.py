import argparse
from datetime import datetime
from os import path
from typing import List

from sqlalchemy import MetaData

from todb.config import ToDbConfig
from todb.data_types import parse_model_file, ConfColumn
from todb.db_engine import sql_table_from_columns, get_db_engine
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

    if args.config is not None and path.exists(args.config) \
            and args.model is not None and path.exists(args.model) \
            and args.input is not None and path.exists(args.input):
        config = ToDbConfig.from_file(args.config)  # type: ToDbConfig
        print("Parsed config to: {}".format(config))

        columns = parse_model_file(args.model)
        print("Parsed model columns: {}".format(columns))

        table_name = path.basename(args.input)
        _register_db_tables(table_name, columns, config)

    print("Done in {:2.3f}s!".format(seconds_between(start_time)))


def _register_db_tables(table_name: str, columns: List[ConfColumn], config: ToDbConfig):
    db_engine = get_db_engine(todb_config=config)
    sql_meta = MetaData()
    sql_table_from_columns(sql_meta, table_name, columns)
    sql_meta.create_all(db_engine)


if __name__ == "__main__":
    cli_main()
