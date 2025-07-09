# DTS transfer status

This directory contains the DTS transfer status tool code. The tool is used to display table-level transfer statuses for a DTS migration.

## Usage

### To list the transfer configurations created in the selected project and locations run:

`./dwh-dts-transfer-status --list-transfer-configs --project-id=PROJECT_ID --location=LOCATION`

### To get the latest status of each table included in the selected configuration run:

`./dwh-dts-transfer-status --list-status-for-config --project-id=PROJECT_ID --config-id=CONFIG_ID --location=LOCATION`

### To get the latest status of each table in the selected database throughout all the configurations run:

`./dwh-dts-transfer-status --list-status-for-database --project-id=PROJECT_ID --location=LOCATION --database=DATABASE`