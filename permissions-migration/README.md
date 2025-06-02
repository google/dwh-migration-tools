# Permission migration tool
This directory contains the permissions migration tool. The tool can be used to map principals and permissions/policies from HDFS and Ranger (HDFS and Hive plugins) into GCP principals and IAM roles. The tool is mainly intended to be used with other tools do provide the end to end migration process:
1. dwh-dumper is used to get information from HDFS, Ranger and Hive
2. translation service is used to provide the Hive to GCS mappings
3. BigQuery DTS is used to transfer the metadata (table schemas) and data to GCP

While it is possible to use permissions-migration standalone, using it with the migration service tools (available soon) provides a much better user experience.

## Usage

### Map principals

```aiexclude
./dwh-permissions-migration expand \
    --principal-ruleset gs://MIGRATION_BUCKET/principals-ruleset.yaml \
    --hdfs-dumper-output gs://MIGRATION_BUCKET/hdfs-dumper-output.zip \
    --ranger-dumper-output gs://MIGRATION_BUCKET/ranger-dumper-output.zip \
    --output-principals gs://MIGRATION_BUCKET/principals.yaml
```

### Map permissions

```aiexclude
./dwh-permissions-migration build \
    --permissions-ruleset gs://MIGRATION_BUCKET</var>/permissions-config.yaml \
    --tables gs://MIGRATION_BUCKET</var>/tables/ \
    --principals gs://MIGRATION_BUCKET</var>/principals.yaml \
    --ranger-dumper-output gs://MIGRATION_BUCKET</var>/ranger-dumper-output.zip \
    --hdfs-dumper-output gs://MIGRATION_BUCKET</var>/hdfs-dumper-output.zip \
    --output-permissions gs://MIGRATION_BUCKET</var>/permissions.yaml
```

### Apply permissions

Note: applying the permissions requires a connection to GCP and a gcloud user with the correct roles. Alternatively a provided python script can be used to generate a terraform script to be applied manually.
```aiexclude
./dwh-permissions-migration apply \
--permissions gs://MIGRATION_BUCKET/permissions.yaml
```