This subproject contains the thrift files and the generated Java source files.

Installing thrift can be a barrier and we rarely need to update thrift files.
Hence, the generated code is checked in and only re-generated manually when
a change to the thrift files is made.

When updating any thrift file, run the following task and commit the generated
files together with the change of the thrift files

```
./gradlew :dumper:lib-ext-hive-metastore:generateThrift
```
