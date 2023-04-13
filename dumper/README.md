# BigQuery Migration Service Metadata and Log Dumper

This directory contains the Metadata and Log Dumper, a command line tool for
connecting to an existing database and generating an archive of DDL metadata or
logs. This tool generates archives in a format suitable for consumption by the
[BigQuery Migration Service's][BQMS] Assessment or Translation Service.

The Dumper is a Java tool. **[Download the latest cross-platform release zip `dwh-migration-tools-vX.X.X.zip`.](https://github.com/google/dwh-migration-tools/releases/latest)**

To get started using the Dumper, read
[the documentation](https://cloud.google.com/bigquery/docs/generate-metadata).


[BQMS]: https://cloud.google.com/bigquery/docs/migration-intro
