# Contributing

## Requirements

- [Poetry][poetry] (analyze, test)
- [npm][npm] (analyze)
- [addlicense][addlicense] (analyze)
- [Google Cloud CLI][google-cloud-cli] (test, build)

## Install

```shell
poetry install
```

## Analyze (format, lint, type check)

```shell
poetry shell
nox -t analyze
```

## Test

### Unit

```shell
poetry shell
nox -s tests -- unit
```

### Integration

```shell
poetry shell
BQMS_VERBOSE="False" \
BQMS_MULTITHREADED="True" \
BQMS_PROJECT="<YOUR_TEST_PROJECT>" \
BQMS_GCS_BUCKET="<YOUR_TEST_BUCKET>" \
nox -s tests -- integration
```

### Unit and Integration

```shell
poetry shell
nox -s tests
```

### Logs

To view all logs while running tests:

```shell
poetry shell
nox -s tests --verbose -- integration pytest_verbose
```

## Build

```shell
export BQMS_ARTIFACT_PROJECT="ajwelch-bqms-test-004"
export BQMS_ARTIFACT_REPOSITORY_ID="bqms"
export BQMS_ARTIFACT_REPOSITORY_REGION="us-east4"
IFS=''
tag=("$BQMS_ARTIFACT_REPOSITORY_REGION-docker.pkg.dev/$BQMS_ARTIFACT_PROJECT/"
     "$BQMS_ARTIFACT_REPOSITORY_ID/bqms:latest")
export BQMS_ARTIFACT_TAG="${tag[*]}"

# TODO: How do we make this public and publish via CI/CD?
gcloud --project=$BQMS_ARTIFACT_PROJECT artifacts repositories create \
    $BQMS_ARTIFACT_REPOSITORY_ID --repository-format=docker \
    --location=$BQMS_ARTIFACT_REPOSITORY_REGION

gcloud --project=$BQMS_ARTIFACT_PROJECT builds submit -t $BQMS_ARTIFACT_TAG
```

## Docs

Be sure to update the [README](./README.md) and [Codelab][codelab] if necessary.

<!-- markdownlint-disable line-length -->

[poetry]: https://python-poetry.org/docs/#installation
[npm]: https://docs.npmjs.com/downloading-and-installing-node-js-and-npm
[addlicense]: https://github.com/google/addlicense
[google-cloud-cli]: https://cloud.google.com/sdk/docs/install
[codelab]: https://g3doc.corp.google.com/cloud/helix/edwmigration/translation/g3doc/codelab/index.md?cl=head
