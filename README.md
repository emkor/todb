# todb (work in progress) [![Build Status](https://travis-ci.com/emkor/todb.svg?branch=master)](https://travis-ci.com/emkor/todb)
importing csv data into any SQL DB in a smart way

## Current status
- supported input files:
    - flat-structure (CSVs, TSVs etc.)
- supported output databases:
    - Postgres
    - Sqlite
- performance (time taken / input file size):
    - on quad-core CPU laptop, SSD:
        - local database and 3-columns of data in 8 MB file: 800-950 kB/s
        - local database and 8-columns of data in >400 MB file: ~1600 kB/s

## Usage
- configure `file` section of `resources/config.json` to match structure of your input file
- provide credentials to your DB by configuring `db` section of `resources/config.json`
- describe your SQL table in JSON file (example in `resources/example_model.json`)
- run with `todb <config.json> <model.json> <input_file.csv>`
