# Contributing

## Requirements

- [Poetry][poetry] (analyze, test)
- [npm][npm] (analyze)
- [addlicense][addlicense] (analyze)
- [Google Cloud CLI][google-cloud-cli] (test)

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
BQMS_VERBOSE="False" \
BQMS_MULTITHREADED="True" \
BQMS_PROJECT="<YOUR_TEST_PROJECT>" \
BQMS_GCS_BUCKET="<YOUR_TEST_BUCKET>" \
nox -s tests
```

### Logs

To view all logs while running tests:

```shell
poetry shell
nox -s tests --verbose -- unit pytest_verbose
```

## Docs

Be sure to update the [README](./README.md) and [Codelab][codelab] if necessary.

<!-- markdownlint-disable line-length -->

[poetry]: https://python-poetry.org/docs/#installation
[npm]: https://docs.npmjs.com/downloading-and-installing-node-js-and-npm
[addlicense]: https://github.com/google/addlicense
[google-cloud-cli]: https://cloud.google.com/sdk/docs/install
[codelab]: https://g3doc.corp.google.com/cloud/helix/edwmigration/translation/g3doc/codelab/index.md?cl=head
