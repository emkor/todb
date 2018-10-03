import argparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Import CSV/TSV files into any SQL DB system')
    parser.add_argument('config', type=str, help='Use given configuration file')
    parser.add_argument('input', type=str, help='A CSV/TSV file to import into DB')
    return parser.parse_args()


def cli_main() -> None:
    args = parse_args()
    main(args)


def main(args: argparse.Namespace) -> None:
    print("Running with: {}!".format(args))


if __name__ == "__main__":
    cli_main()
