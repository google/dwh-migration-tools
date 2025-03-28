# BigQuery Migration Service Dbsync

This directory contains the source code for Dbsync. A set of tools and library
for syncing files into Google Cloud Storage using the `rsync` algorithm. This
project is in the process of development.

Compiling the code from source requires `Java 8`, running the tools requires
`Java 8` or higher. To check Java version run the command`java -version` or
refer to Java vendor documentation.

## Gcysnc

This tool is in preview mode, it's being actively developed.

### Build the Gcsync Client ###

    ./gradlew :dbsync:gcsync:build

### Run the Gcsync Client ###

    java -jar gcsync-all.jar --target_bucket <target_bucket> --tmp_bucket <tmp_bucket> --project <project_id> --location <location> --source_directory <source_directory> --task_timeout <task_time_out>

- tmp_bucket: A GCS bucket used to store the jar and staging files such as the
  checksum file
- target_bucket: The GCS bucket that files shall be uploaded/rsynced to
- project: ID of the GCP project that the tool is run against
- location: The location the GCP Cloud run jobs should run at. For
  instance `us-central1`
- source_directory: Optional, the directory the tools scans for files to
  upload/rsync
- task_timeout: The maximum timeout 