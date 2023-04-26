# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Dataclass, schema and validator for translation type."""
from dataclasses import dataclass
from typing import Mapping

from google.cloud import bigquery_migration_v2
from marshmallow import Schema, fields, post_load, validate


class _TranslationTypeSchema(Schema):
    """Schema and validator for TranslationType."""

    _dialect_map = {
        "Translation_Teradata2BQ": {
            "source": bigquery_migration_v2.Dialect(
                teradata_dialect=bigquery_migration_v2.TeradataDialect(
                    mode=bigquery_migration_v2.TeradataDialect.Mode.SQL
                )
            ),
            "target": bigquery_migration_v2.Dialect(
                bigquery_dialect=bigquery_migration_v2.BigQueryDialect()
            ),
        },
        "Translation_Redshift2BQ": {
            "source": bigquery_migration_v2.Dialect(
                redshift_dialect=bigquery_migration_v2.RedshiftDialect()
            ),
            "target": bigquery_migration_v2.Dialect(
                bigquery_dialect=bigquery_migration_v2.BigQueryDialect()
            ),
        },
        "Translation_Bteq2BQ": {
            "source": bigquery_migration_v2.Dialect(
                teradata_dialect=bigquery_migration_v2.TeradataDialect(
                    mode=bigquery_migration_v2.TeradataDialect.Mode.BTEQ
                )
            ),
            "target": bigquery_migration_v2.Dialect(
                bigquery_dialect=bigquery_migration_v2.BigQueryDialect()
            ),
        },
        "Translation_Oracle2BQ": {
            "source": bigquery_migration_v2.Dialect(
                oracle_dialect=bigquery_migration_v2.OracleDialect()
            ),
            "target": bigquery_migration_v2.Dialect(
                bigquery_dialect=bigquery_migration_v2.BigQueryDialect()
            ),
        },
        "Translation_HiveQL2BQ": {
            "source": bigquery_migration_v2.Dialect(
                hiveql_dialect=bigquery_migration_v2.HiveQLDialect()
            ),
            "target": bigquery_migration_v2.Dialect(
                bigquery_dialect=bigquery_migration_v2.BigQueryDialect()
            ),
        },
        "Translation_SparkSQL2BQ": {
            "source": bigquery_migration_v2.Dialect(
                sparksql_dialect=bigquery_migration_v2.SparkSQLDialect()
            ),
            "target": bigquery_migration_v2.Dialect(
                bigquery_dialect=bigquery_migration_v2.BigQueryDialect()
            ),
        },
        "Translation_Snowflake2BQ": {
            "source": bigquery_migration_v2.Dialect(
                snowflake_dialect=bigquery_migration_v2.SnowflakeDialect()
            ),
            "target": bigquery_migration_v2.Dialect(
                bigquery_dialect=bigquery_migration_v2.BigQueryDialect()
            ),
        },
        "Translation_Netezza2BQ": {
            "source": bigquery_migration_v2.Dialect(
                netezza_dialect=bigquery_migration_v2.NetezzaDialect()
            ),
            "target": bigquery_migration_v2.Dialect(
                bigquery_dialect=bigquery_migration_v2.BigQueryDialect()
            ),
        },
        "Translation_AzureSynapse2BQ": {
            "source": bigquery_migration_v2.Dialect(
                azure_synapse_dialect=bigquery_migration_v2.AzureSynapseDialect()
            ),
            "target": bigquery_migration_v2.Dialect(
                bigquery_dialect=bigquery_migration_v2.BigQueryDialect()
            ),
        },
        "Translation_Vertica2BQ": {
            "source": bigquery_migration_v2.Dialect(
                vertica_dialect=bigquery_migration_v2.VerticaDialect()
            ),
            "target": bigquery_migration_v2.Dialect(
                bigquery_dialect=bigquery_migration_v2.BigQueryDialect()
            ),
        },
        "Translation_SQLServer2BQ": {
            "source": bigquery_migration_v2.Dialect(
                sql_server_dialect=bigquery_migration_v2.SQLServerDialect()
            ),
            "target": bigquery_migration_v2.Dialect(
                bigquery_dialect=bigquery_migration_v2.BigQueryDialect()
            ),
        },
        "Translation_Presto2BQ": {
            "source": bigquery_migration_v2.Dialect(
                presto_dialect=bigquery_migration_v2.PrestoDialect()
            ),
            "target": bigquery_migration_v2.Dialect(
                bigquery_dialect=bigquery_migration_v2.BigQueryDialect()
            ),
        },
        "Translation_MySQL2BQ": {
            "source": bigquery_migration_v2.Dialect(
                mysql_dialect=bigquery_migration_v2.MySQLDialect()
            ),
            "target": bigquery_migration_v2.Dialect(
                bigquery_dialect=bigquery_migration_v2.BigQueryDialect()
            ),
        },
        "Translation_Postgresql2BQ": {
            "source": bigquery_migration_v2.Dialect(
                postgresql_dialect=bigquery_migration_v2.PostgresqlDialect()
            ),
            "target": bigquery_migration_v2.Dialect(
                bigquery_dialect=bigquery_migration_v2.BigQueryDialect()
            ),
        },
        "Translation_BQ2SparkSQL": {
            "source": bigquery_migration_v2.Dialect(
                bigquery_dialect=bigquery_migration_v2.BigQueryDialect()
            ),
            "target": bigquery_migration_v2.Dialect(
                sparksql_dialect=bigquery_migration_v2.SparkSQLDialect()
            ),
        },
    }

    name = fields.String(
        required=True,
        validate=validate.OneOf(_dialect_map.keys()),
    )

    @post_load
    def build(self, data, **kwargs):  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        name = data["name"]
        source_dialect = self._dialect_map[name]["source"]
        target_dialect = self._dialect_map[name]["target"]
        return TranslationType(
            name=name, source_dialect=source_dialect, target_dialect=target_dialect
        )


@dataclass
class TranslationType:
    """The type of translation to perform, including source and target dialect.

    Example:
        .. code-block::

            translation_type = TranslationType.from_mapping({"name": "HiveQL2BQ"})

    Attributes:
        name: A string representing the name of the translation type.
        source_dialect: A bigquery_migration_v2.Dialect representing the source
            dialect of the translation type.
        target_dialect: A bigquery_migration_v2.Dialect representing the target
            dialect of the translation type.
    """

    name: str
    source_dialect: bigquery_migration_v2.Dialect
    target_dialect: bigquery_migration_v2.Dialect

    @staticmethod
    def from_mapping(mapping: Mapping[str, object]) -> "TranslationType":
        """Factory method for creating a TranslationType from a Mapping.

        Args:
            mapping: A Mapping of the form: {"name": "HiveQL2BQ"}.

        Returns:
            A TranslationType instance.
        """
        translation_type: TranslationType = _TranslationTypeSchema().load(mapping)
        return translation_type
