# todb (work in progress) [![Build Status](https://travis-ci.com/emkor/todb.svg?branch=master)](https://travis-ci.com/emkor/todb)
importing csv data into any SQL DB in a smart way

## Current status
- supported input files:
    - flat-structure (CSVs, TSVs etc.)
- supported output databases:
    - anything SQLalchemy can use (tested with PostgreSQL and SQLite)
- performance (time taken / input file size):
    - on quad-core CPU laptop, SSD:
        - local PostgreSQL, 3 columns of data in 8 MB file: `800-950 kB/s`
        - local PostgreSQL, 9 columns of data in ~120MB file with 256 kB of chunk size: `5200-5900 kB/s`

## Usage
- describe your target SQL table in JSON file (example: `resources/example_model.json` which maps `resources/example_input.csv`)
- run with `todb <path to model.json> <path to input_file.csv> --sql_db <sqlalchemy formatted SQL DB URL>`
- try `todb -h` for detailed options

## Full example
- `todb example_model.json example_input.csv postgresql://user:secret@localhost:5432/default --table example.csv --failures failures.csv --proc 4 --chunk 128`
    - this will import `example_input.csv` file using mapping from `example_model.json`
    - into table named `example.csv` on localhost-run PostgreSQL
    - while logging incorrect rows in file `failures.csv`
    - using 4 CPU cores and importing file into batches of size roughly `128 kB`