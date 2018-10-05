# todb (work in progress) [![Build Status](https://travis-ci.com/emkor/todb.svg?branch=master)](https://travis-ci.com/emkor/todb)
experimental project: importing csv data into any SQL DB in a smart way

## Current status
- supported input files:
    - flat-structure (CSVs, TSVs etc.)
- supported output databases:
    - Postgres
- performance (time taken / input file size):
    - on quad-core CPU laptop, SSD, local database and 3-columns of data: 800-950 kB/s
