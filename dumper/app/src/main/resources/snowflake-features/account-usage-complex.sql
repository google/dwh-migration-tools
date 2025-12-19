-- Copyright 2022-2025 Google LLC
-- Copyright 2013-2021 CompilerWorks
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
WITH clustering_classified AS (
  SELECT
    CLUSTERING_KEY,
      CASE
        WHEN REGEXP_LIKE(
        -- After normalization, check if the key is ONLY a comma-separated list of identifiers
          REGEXP_REPLACE( -- (3) remove whitespace and double quotes
            REGEXP_REPLACE( -- (2) remove trailing closing parenthesis ")"
              REGEXP_REPLACE( -- (1) remove leading "LINEAR("
                CLUSTERING_KEY,
                '^\\s*LINEAR\\s*\\(\\s*', -- ^ start of string, optional spaces, "LINEAR", optional spaces, "("
                '',  1, 1, 'i'
              ),
              '\\)\\s*$', -- trailing ")" at end of string + optional trailing spaces
              '', 1, 1, 'i'
            ),
            '[\\s"]', -- any whitespace (\s) or double-quote (")
            '',  1, 0, 'is'  -- 'i' = case-insensitive, 's' = dot matches newline (safe for multi-line)
          ),

          '^[A-Z0-9_\\$\\.,]+$', -- only identifier characters: letters/digits/_/$ plus "." and ","
          'i'
        )
        THEN 'BY_COLUMNS'
        ELSE 'BY_EXPRESSION'
      END AS key_type
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
    WHERE DELETED IS NULL
      AND CLUSTERING_KEY IS NOT NULL
  )
  -- clustering by columns
  SELECT 'storage_table_layout', 'clustering_by_columns', COUNT(*), ''
  FROM clustering_classified WHERE key_type='BY_COLUMNS'

  UNION ALL
  -- clustering by expressions
  SELECT'storage_table_layout', 'clustering_by_expressions', COUNT(*), ''
  FROM clustering_classified WHERE key_type='BY_EXPRESSION'

  UNION ALL
  -- cursors in procedures
  SELECT 'sql',
    'cursors_in_procedures',
    COUNT_IF(
      REGEXP_LIKE(
        procedure_definition,
        $$.*(CURSOR[[:space:]]+FOR|DECLARE[[:space:]]+[[:alnum:]_]+[[:space:]]+CURSOR|OPEN[[:space:]]+[[:alnum:]_]+|FETCH[[:space:]]+[[:alnum:]_]+|CLOSE[[:space:]]+[[:alnum:]_]+).*$$,
        'is'
      )
    ),
    ''
  FROM snowflake.account_usage.procedures
  WHERE deleted IS NULL
    AND procedure_definition IS NOT NULL
    AND procedure_definition <> '';
