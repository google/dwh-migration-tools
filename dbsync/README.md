# BigQuery Migration Service Dbsync

This directory contains the source code for Dbsync. A set of tools and library
for syncing files into Google Cloud Storage using the `rsync` algorithm. This
project is in the process of development.

Compiling the code from source requires `Java 8`, running the tools requires
`Java 8` or higher. To check Java version run the command`java -version` or
refer to Java vendor documentation.

## Contributing

Before submitting any code changes, please run the formatter

    java -jar dbsync/google-java-format-1.7-all-deps.jar -i $(git ls-files dbsync | grep '\.java$')

## Gcysnc

This tool is in preview mode, it's being actively developed.

### Build the Gcsync Client ###

    ./gradlew :dbsync:gcsync:build

You migh need to install `protobuf-compiler` if that's not there already. See
this [page](https://protobuf.dev/installation/) on how to install it.

### Run the Gcsync Client ###

After you successfully compile the source, you will find a fat jar named as gcsync-all.jar under

    dbsync/gcsync/build/libs

You can then run the tool with the following command:

    java -jar gcsync-all.jar --target_bucket <target_bucket> --tmp_bucket <tmp_bucket> --project <project_id> --location <location> --source_directory <source_directory> --task_timeout <task_time_out> –num_concurrent_tasks <num_concurrent_tasks>

- tmp_bucket: A GCS bucket used to store the jar and staging files such as the
  checksum file
- target_bucket: The GCS bucket that files shall be uploaded/rsynced to
- project: ID of the GCP project that the tool is run against
- location: The location the GCP Cloud run jobs should run at. For
  instance `us-central1`
- source_directory: Optional. The directory the tools scans against for
  files to upload/rsync. It will scan the directory the tool is running at
  if not specified.
- task_timeout: Optional. The maximum timeout for the cloud_run task. For
  instance, `7200s`. The default timeout would be 1hr if not specified
- num_concurrent_tasks: Optional. If specified, the tool will distribute files
  into `n` buckets and start `n` cloud run tasks to rsync the files. This could
  improve the total duration of the process when there are multiple files to rsync.
  Uploading is still done in a single thread given that parallelism would likely not
  help when network bandwidth is the bottleneck
