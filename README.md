# Athena input plugin for Embulk

[![Gem Version](https://badge.fury.io/rb/embulk-input-athena.svg)](https://badge.fury.io/rb/embulk-input-athena)
[![MIT License](http://img.shields.io/badge/license-MIT-blue.svg?style=flat)](LICENSE)

//TODO:
https://github.com/dtan4/terraforming

Athena input plugin for Embulk loads records from Athena(AWS).

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Configuration

* **driver_path**: path to the jar file of the Athena JDBC driver. If not set, the bundled JDBC driver(AthenaJDBC41-1.1.0.jar) will be used. (string)
* **database**: description (string, required)
* **athena_url**: description (string, required)
* **s3_staging_dir**: The S3 location to which your query output is written, for example s3://query-results-bucket/folder/. (string, required)
* **access_key**: description (string, required)
* **secret_key**: description (string, required)
* **query**: description (string, required)
* **columns**: description (string, required)
* **options**: description (string, default: {})

## Example

```yaml
in:
  type: athena
  database: log_test
  athena_url: "jdbc:awsathena://athena.ap-northeast-1.amazonaws.com:443"
  s3_staging_dir: "s3://aws-athena-query-results-11111111111-ap-northeast-1/"
  access_key: ""
  secret_key: ""
  query: |
    select uid, created_at from log_test.sample
  columns:
    - {name: uid, type: string}
    - {name: created_at, type: timestamp}
```

## Build

```bash
$ docker-compose up -d
$ docker-compose exec embulk bash
embulk>$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
