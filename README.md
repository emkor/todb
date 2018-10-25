# todb (work in progress) [![Build Status](https://travis-ci.com/emkor/todb.svg?branch=master)](https://travis-ci.com/emkor/todb)
Simple tool for importing large CSV/TSV-file data into SQL databases, focused on performance

## Quick example
`todb model.json file.csv postgresql://user:password@localhost:5432/dbname` where:
- `file.csv` has rows:
```csv
timestamp;parameter;value
30 Aug 2018 11:01;cpu_usage_perc;0.05
30 Aug 2018 11:01;mem_usage_perc;0.21
30 Aug 2018 11:06;cpu_usage_perc;0.05
30 Aug 2018 11:06;mem_usage_perc;0.21
```
- `model.json` describes file and target table:
```json
{
    "file": {
        "encoding": "utf-8",
        "has_header": true,
        "row_delimiter": "\n",
        "cell_delimiter": ";"
    },
    "columns": {
        "Timestamp": {
            "input_file_column": 0,
            "type": "datetime",
            "nullable": false,
            "index": true,
            "unique": false
        },
        "Parameter": {
            "input_file_column": 1,
            "type": "string",
            "nullable": false,
            "index": true,
            "unique": false
        },
        "Value": {
            "input_file_column": 3,
            "type": "float",
            "nullable": true,
            "index": false,
            "unique": false
        }
    },
    "primary_key": ["Timestamp", "Parameter"]
}
```
- `postgresql://user:password@localhost:5432/dbname` is an example of sqlalchemy [database URL](https://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls);
- try `todb -h` for detailed options

## Features
- supports flat-structure files (CSVs, TSVs etc.)
- supports any SQL database that sqlalchemy can use ()tested with PostgreSQL and SQLite)
- automatically recognizes date/time format (using `python-dateutil`)
- supports SSL connection using CA certificate file
- performance (time taken / input file size):
    - quad-core CPU laptop with SSD as a client, PostgreSQL@localhost, 120MB CSV file (9 columns, one of them being datetime); median import time:
        -`pandas`: `read_csv` and `to_sql` methods with specified `dtype`: `~25.271s` (`4.74 MB/s`)
        - `todb`: with chunk size of:
            - `32 kB`: `42.70s` (`2.81 MB/s`)
            - `128 kB`: `23.04s` (`5.21 MB/s`)
            - `512 kB`: `17.50s` (`6.85 MB/s`)
            - `2048 kB`: `16.53s` (`7.26 MB/s`)

## Usage
- describe your target SQL table in JSON file (example: `resources/example_model.json` which maps `resources/example_input.csv`)
- run: `todb <path to input_file.csv> <path to model.json> <sqlalchemy formatted SQL DB URL>`
    
## JSON model file structure
Model file describes your CSV/TSV file structure; consists of three sections:
- `file`
    - parameters are self-explanatory, describes input file encoding, separators etc.
    - see `resources/example_model.json`
- `columns`
    - object describing target SQL table columns and their types and attributes
    - each object here is `"column_name": {"input_file_column": ..., "type": ..., "unique": true, "index": true, "nullable": false}, ...` etc.
    - `input_file_column` is integer, zero-based index of column in input CSV/TSV file to map into the SQL column; if set to `null`, all the column values will be `NULL`; this requires attribute `nullable=true`
    - `type` is one of currently supported types: `bool`, `string`, `int`, `bigint`, `float`, `datetime`, `date`, `time`
    - `unique`, `index` and `nullable` boolean-flags are self-explanatory
- `primary_key`
    - in case of string value of `autoincrement`, SQL table will have additional `ID` column as a primary key of type `Integer` and with `auto-increment` on
    - in case of other string value, it's treated as existing column name defined under `columns`, and this column will become primary key of a table
    - in case of array of strings value, primary key will be clustered key as combination of columns defined under `columns` in defined order