# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""TranslationType Enum."""
from enum import Enum

from google.cloud import bigquery_migration_v2


class TranslationType(Enum):
    """An Enum representing BQMS translation types."""

    # Uppercase is not used here to match the BQMS API.
    # pylint: disable=invalid-name

    Translation_AzureSynapse2BQ = bigquery_migration_v2.Dialect(
        azure_synapse_dialect=bigquery_migration_v2.AzureSynapseDialect()
    )
    Translation_Bteq2BQ = bigquery_migration_v2.Dialect(
        teradata_dialect=bigquery_migration_v2.TeradataDialect(
            mode=bigquery_migration_v2.TeradataDialect.Mode.BTEQ
        )
    )
    Translation_HiveQL2BQ = bigquery_migration_v2.Dialect(
        hiveql_dialect=bigquery_migration_v2.HiveQLDialect()
    )
    Translation_Netezza2BQ = bigquery_migration_v2.Dialect(
        netezza_dialect=bigquery_migration_v2.NetezzaDialect()
    )
    Translation_Oracle2BQ = bigquery_migration_v2.Dialect(
        oracle_dialect=bigquery_migration_v2.OracleDialect()
    )
    Translation_Presto2BQ = bigquery_migration_v2.Dialect(
        presto_dialect=bigquery_migration_v2.PrestoDialect()
    )
    Translation_Redshift2BQ = bigquery_migration_v2.Dialect(
        redshift_dialect=bigquery_migration_v2.RedshiftDialect()
    )
    Translation_Snowflake2BQ = bigquery_migration_v2.Dialect(
        snowflake_dialect=bigquery_migration_v2.SnowflakeDialect()
    )
    Translation_SparkSQL2BQ = bigquery_migration_v2.Dialect(
        sparksql_dialect=bigquery_migration_v2.SparkSQLDialect()
    )
    Translation_Teradata2BQ = bigquery_migration_v2.Dialect(
        teradata_dialect=bigquery_migration_v2.TeradataDialect(
            mode=bigquery_migration_v2.TeradataDialect.Mode.SQL
        )
    )
    Translation_Vertica2BQ = bigquery_migration_v2.Dialect(
        vertica_dialect=bigquery_migration_v2.VerticaDialect()
    )
    Translation_SQLServer2BQ = bigquery_migration_v2.Dialect(
        sql_server_dialect=bigquery_migration_v2.SQLServerDialect()
    )

    # pylint: enable=invalid-name

    def __repr__(self) -> str:
        return self.name
