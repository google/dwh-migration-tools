# BigQuery Migration Service Metadata and Log Dumper

This directory contains the Metadata and Log Dumper, a command line tool for
connecting to an existing database and generating an archive of DDL metadata or
logs. This tool generates archives in a format suitable for consumption by the
[BigQuery Migration Service's][BQMS] Assessment or Translation Service.

The Dumper is a Java tool. **[Download the latest cross-platform release zip `dwh-migration-tools-vX.X.X.zip`.](https://github.com/google/dwh-migration-tools/releases/latest)**

The Dumper requires Java 8 or higher. To check Java version run the command
`java -version` or refer to Java vendor documentation. Third party JDBC drivers
that might impose additional restrictions on Java versions. Refer to the JDBC
driver's manual for details.

To get started using the Dumper, read
[the documentation](https://cloud.google.com/bigquery/docs/generate-metadata).


[BQMS]: https://cloud.google.com/bigquery/docs/migration-intro
