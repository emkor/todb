# todb (work in progress) [![Build Status](https://travis-ci.com/emkor/todb.svg?branch=master)](https://travis-ci.com/emkor/todb)
Simple tool for importing (even big) CSV/TSV data into SQL databases, focused on automatic format inference and performance

## Current status
- supported file formats:
    - flat-structure (CSVs, TSVs etc.)
    - any file encoding (reads in binary)
- supported databases:
    - theoretically, anything sqlalchemy can use
    - tested with PostgreSQL and SQLite
- features:
    - parametrized CSV/TSV model
    - automatic date and time format recognition (using `python-dateutil`)
    - uses `multiprocessing` and Python generators to stream data into DB efficiently
- performance (time taken / input file size):
    - on quad-core CPU laptop, SSD:
        - local PostgreSQL, 3 columns of data in 8 MB file: `800-950 kB/s`
        - local PostgreSQL, 9 columns of data in ~120MB file with 256 kB of chunk size: `5200-5900 kB/s`

## Usage
- describe your target SQL table in JSON file (example: `resources/example_model.json` which maps `resources/example_input.csv`)
- run: `todb <path to model.json> <path to input_file.csv> <sqlalchemy formatted SQL DB URL>`
- try `todb -h` for detailed options

## Full example
- `todb example_model.json example_input.csv postgresql://user:secret@localhost:5432/default --table example.csv --failures failures.csv --proc 4 --chunk 128`
    - this will import `example_input.csv` file using mapping from `example_model.json`
    - into table named `example.csv` on localhost-run PostgreSQL
    - while logging incorrect rows in file `failures.csv`
    - using 4 CPU cores and importing file into batches of size roughly `128 kB`
    
## JSON model file structure
Model file describes your CSV/TSV file structure; consists of three sections:
- `file`
    - parameters are self-explanatory, describes input file encoding, separators etc.
    - see `resources/example_model.json`
- `columns`
    - object describing columns and their types and attributes
    - each object here is `"column_name": {"input_file_column": ..., "type": ..., "unique": true, "index": true, "nullable": false}, ...` etc.
    - `input_file_column` is integer, zero-based index of column in input CSV/TSV file to map into the SQL column
    - `type` is one of currently supported types: `bool`, `string`, `int`, `bigint`, `float`, `datetime`, `date`, `time`
    - `unique`, `index` and `nullable` boolean-flags are self-explanatory
- `primary_key`
    - in case of value of `autoincrement`, SQL table will have additional `ID` column as a primary key of type `Integer` and with `auto-increment` on
    - in case of other `string` value, it's treated as existing column name defined under `columns`, and this column will become primary key of a table
    - in case of array of strings value, primary key will be clustered key as combination of columns defined under `columns` in defined order