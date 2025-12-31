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
WITH
  tables AS (
    SELECT IS_ICEBERG, IS_DYNAMIC, IS_HYBRID, AUTO_CLUSTERING_ON, RETENTION_TIME
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
    WHERE DELETED IS NULL
  ),
  columns AS (
    SELECT DATA_TYPE, DATETIME_PRECISION
    FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS
    WHERE DELETED IS NULL
  ),
  procedures AS (
    SELECT PACKAGES, PROCEDURE_LANGUAGE
    FROM SNOWFLAKE.ACCOUNT_USAGE.PROCEDURES
    WHERE DELETED IS NULL
  ),
  functions AS (
    SELECT PACKAGES, FUNCTION_LANGUAGE
    FROM SNOWFLAKE.ACCOUNT_USAGE.FUNCTIONS
    WHERE DELETED IS NULL
  ),
  languages AS (
    SELECT column1 AS language
    FROM VALUES ('JAVA'), ('JAVASCRIPT'), ('SQL'), ('PYTHON')
  ),
  data_types AS (
    SELECT column1 AS data_type
    FROM VALUES ('VARIANT'), ('OBJECT'), ('VECTOR'), ('GEOMETRY'),
      ('TIMESTAMP_LTZ'), ('TIMESTAMP_NTZ'), ('TIMESTAMP_TZ')
  )
  -- iceberg tables
  SELECT 'storage_table_layout', 'iceberg_columns', COUNT(*), ''
  FROM tables WHERE IS_ICEBERG = 'YES'

  UNION ALL
  -- dynamic tables
  SELECT 'storage_table_layout', 'dynamic_table_columns', COUNT(*), ''
  FROM tables WHERE IS_DYNAMIC = 'YES'

  UNION ALL
  -- hybrid tables
  SELECT 'storage_table_layout', 'hybrid_table_columns', COUNT(*), ''
  FROM tables WHERE IS_HYBRID = 'YES'

  UNION ALL
  -- automatic clustering tables
  SELECT 'services', 'automatic_clustering', COUNT(*), ''
  FROM tables WHERE AUTO_CLUSTERING_ON = 'YES'

  UNION ALL
  -- time travel, retention period >= 7 and <= 14
  SELECT'services', 'time_travel_between_7d_14d', COUNT(*), ''
  FROM tables WHERE RETENTION_TIME >= 7 AND RETENTION_TIME <= 14

  UNION ALL
  -- time travel, retention period >= 14
  SELECT'services', 'time_travel_gt_14d', COUNT(*), ''
  FROM tables WHERE RETENTION_TIME >= 7 AND RETENTION_TIME >= 14

  UNION ALL
  -- parquet file format
  SELECT 'storage', 'parquet_format', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.FILE_FORMATS
  WHERE DELETED IS NULL AND UPPER(FILE_FORMAT_TYPE) = 'PARQUET'

  UNION ALL
  -- masking policies
  SELECT 'security', 'masking_policies', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES
  WHERE POLICY_KIND = 'MASKING_POLICY'

  UNION ALL
  -- secure views
  SELECT 'security', 'secure_views', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.VIEWS
  WHERE DELETED IS NULL AND IS_SECURE = 'YES'

  UNION ALL
  -- custom roles
  SELECT 'security', 'custom_roles', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES
  WHERE DELETED_ON IS NULL
   AND UPPER(NAME) NOT IN ('ACCOUNTADMIN','SYSADMIN','SECURITYADMIN','USERADMIN','ORGADMIN','PUBLIC')

  UNION ALL
  -- Snowpark Usage
  SELECT 'service', 'snowpark_procedures', COUNT(*), 'PROCEDURES'
  FROM procedures
    WHERE PROCEDURE_LANGUAGE IN ('PYTHON','JAVA','SCALA')
      AND (PACKAGES LIKE '%snowflake-snowpark-python%' OR PACKAGES LIKE '%com.snowflake:snowpark%')
  UNION ALL
  SELECT 'service', 'snowpark_functions', COUNT(*), 'FUNCTIONS'
  FROM functions
    WHERE FUNCTION_LANGUAGE IN ('PYTHON','JAVA','SCALA')
      AND (PACKAGES LIKE '%snowflake-snowpark-python%' OR PACKAGES LIKE '%com.snowflake:snowpark%')

  UNION ALL
  -- Zero copy replication
  SELECT 'service', 'zero_copy_replication', COUNT(*), 'CLONES'
  FROM snowflake.account_usage.table_storage_metrics t1
  JOIN (
    SELECT CLONE_GROUP_ID, MIN(TABLE_CREATED) AS min_created
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
    WHERE CLONE_GROUP_ID IS NOT NULL
    GROUP BY CLONE_GROUP_ID
  ) t2 ON t1.CLONE_GROUP_ID = t2.CLONE_GROUP_ID AND t1.TABLE_CREATED > t2.min_created
  WHERE NOT t1.DELETED

  UNION ALL
  -- Snowpipe
  SELECT 'etl', 'snowpipe', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.PIPES
  WHERE DELETED IS NULL

  UNION ALL
  -- SP - Python, Java, Javascript, SQL
  SELECT 'sql', 'stored_procedures_' || LOWER(languages.language), COALESCE(COUNT(procedures.PROCEDURE_LANGUAGE), 0), ''
  FROM languages
  LEFT JOIN procedures ON UPPER(procedures.procedure_language) = languages.language
  GROUP BY languages.language

  UNION ALL
  -- Functions - Python, Java, Javascript, SQL
  SELECT 'sql', 'functions_' || LOWER(languages.language), COALESCE(COUNT(functions.FUNCTION_LANGUAGE), 0), ''
  FROM languages
  LEFT JOIN functions ON UPPER(functions.function_language) = languages.language
  GROUP BY languages.language

  UNION ALL
  -- data types - variant, object, vector, geometry, timestamp_ltz, timestamp_ntz, timestamp_tz
  SELECT 'data_types', LOWER(data_types.data_type), COALESCE(COUNT(columns.data_type), 0), ''
  FROM data_types
  LEFT JOIN columns  ON UPPER(columns.data_type) = data_types.data_type
  GROUP BY data_types.data_type

  UNION ALL
  -- data types - timestamp with nanoseconds
  SELECT 'data_types', 'timestamp_nano', COUNT(*), ''
  FROM columns
  WHERE DATA_TYPE LIKE 'TIMESTAMP%' AND DATETIME_PRECISION > 6

  UNION ALL
  -- COPY INTO - count executions in the last 30 days
  SELECT 'etl', 'copy_into_last_30_days', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE START_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP())
    AND QUERY_TEXT ILIKE 'COPY INTO %'

  UNION ALL
  -- GET DDL - count executions in the last 30 days
  SELECT 'sql', 'get_ddl_last_30_days', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE START_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP())
    AND QUERY_TEXT ILIKE '%GET_DDL(%'

  UNION ALL
  -- dynamic pivot clause
  SELECT 'sql', 'dynamic_pivot_clause_last_30_days', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE START_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP())
    AND QUERY_TEXT ILIKE '%PIVOT%'
    AND REGEXP_LIKE(QUERY_TEXT, $$.*\bIN\b\s*\(\s*\bANY\b.*$$, 'is')

  UNION ALL
  -- Horizon - Tags exist (catalog metadata is being used)
  SELECT 'governance', 'horizon_tags_defined', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.TAGS
  WHERE DELETED IS NULL

  UNION ALL
  -- Horizon - Tag assignments exist
  SELECT 'governance', 'horizon_tag_assignments', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
  WHERE OBJECT_DELETED IS NULL

  UNION ALL
  -- Horizon - Sensitive data classification was run
  SELECT 'governance', 'horizon_sensitive_data_classification_tables', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_CLASSIFICATION_LATEST

  UNION ALL
  -- Snowpark container services - compute pools
  SELECT 'service', 'snowpark_container_services_compute_pools', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.COMPUTE_POOLS
  WHERE DELETED IS NULL

  UNION ALL
  -- Snowpark container services - services
  SELECT 'service', 'snowpark_container_services_services', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.SERVICES
  WHERE DELETED IS NULL

  UNION ALL
  -- BI - Tableau heuristics
  SELECT 'bi', 'tableau_query_tagged_queries_30d', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    AND QUERY_TAG IS NOT NULL
    AND (
         QUERY_TAG ILIKE '%tableau%'
         OR QUERY_TAG ILIKE '%workbook%'
         OR QUERY_TAG ILIKE '%sheet%'
    )

  UNION ALL
  -- BI - Sigma heuristics
  SELECT 'bi', 'sigma_query_tagged_queries_30d', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    AND QUERY_TAG IS NOT NULL
    AND QUERY_TAG ILIKE '%sigma%';