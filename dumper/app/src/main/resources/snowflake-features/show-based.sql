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
DECLARE
  show_tables_query_id VARCHAR;
  show_dbt_projects_query_id VARCHAR;
  show_warehouses_query_id VARCHAR;
  show_tasks_query_id VARCHAR;
  show_streamlits_query_id VARCHAR;
  show_apps_installed_query_id VARCHAR;
  show_app_packages_query_id VARCHAR;
  final_result RESULTSET;
BEGIN
  -- contains search optimization info
  SHOW TABLES IN ACCOUNT;

  -- Search optimization detection, in some editions, the snowflake flag is not available.
  WITH flattened_show_tables AS (
    SELECT UPPER(flat.key::string) AS column_name,
      flat.value::string AS column_value
    FROM (
      SELECT OBJECT_CONSTRUCT(*) AS table_row_object
      FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    ) tab,
    LATERAL FLATTEN(INPUT => tab.table_row_object) flat
  ),
  aggregated AS (
    SELECT
      MAX(IFF(column_name = 'SEARCH_OPTIMIZATION', 1, 0)) AS has_search_optimization_column,
      SUM(IFF(column_name = 'SEARCH_OPTIMIZATION' AND column_value = 'ON', 1, 0)) AS search_optimization_on_count
    FROM flattened_show_tables
  )
  SELECT 'table', 'search_optimization',  IFF(has_search_optimization_column = 1, search_optimization_on_count, 0), IFF(has_search_optimization_column = 1, '', 'no_column')
  FROM aggregated;

  show_tables_query_id := LAST_QUERY_ID();

  -- Contains DBT Projects info
  SHOW DBT PROJECTS IN ACCOUNT;

  -- DBT Projects
  SELECT 'etl', 'dbt_projects', COUNT(*), 'PROJECTS'
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  show_dbt_projects_query_id := LAST_QUERY_ID();

  -- Contains query acceleration, Gen2 warehouses, snowpark-optimized info
  SHOW WAREHOUSES IN ACCOUNT;

  -- query acceleration
  SELECT 'service', 'query_acceleration', COUNT(*), 'WAREHOUSES'
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
  WHERE UPPER('enable_query_acceleration') = 'true'
  UNION ALL
  -- Gen2 warehouses
  SELECT 'service', 'gen2_warehouses', COUNT(*), 'WAREHOUSES'
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
  WHERE UPPER('resource_constraint') = 'STANDARD_GEN_2'
  UNION ALL
  -- snowpark-optimized
  SELECT 'service', 'snowpark_optimized_warehouses', COUNT(*), 'WAREHOUSES'
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
  WHERE "type" = 'SNOWPARK-OPTIMIZED';

  show_warehouses_query_id := LAST_QUERY_ID();

  -- Contains tasks info
  SHOW TASKS IN ACCOUNT;

  SELECT 'service', 'tasks', COUNT(*), 'TASKS'
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  show_tasks_query_id := LAST_QUERY_ID();

  -- Contains streamlits info
  SHOW STREAMLITS IN ACCOUNT;

  SELECT 'app', 'streamlit', COUNT(*), 'STREAMLITS'
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  show_streamlits_query_id := LAST_QUERY_ID();

  -- Contains native apps installed info
  SHOW APPLICATIONS IN ACCOUNT;

  SELECT 'app', 'native_apps_installed', COUNT(*), ''
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  show_apps_installed_query_id := LAST_QUERY_ID();

  -- Contains native apps packages info
  SHOW APPLICATION PACKAGES IN ACCOUNT;

  SELECT 'app', 'native_app_packages', COUNT(*), ''
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  show_app_packages_query_id := LAST_QUERY_ID();

  final_result := (
    SELECT * FROM TABLE(RESULT_SCAN(:show_tables_query_id))
    UNION ALL
    SELECT * FROM TABLE(RESULT_SCAN(:show_dbt_projects_query_id))
    UNION ALL
    SELECT * FROM TABLE(RESULT_SCAN(:show_warehouses_query_id))
    UNION ALL
    SELECT * FROM TABLE(RESULT_SCAN(:show_tasks_query_id))
    UNION ALL
    SELECT * FROM TABLE(RESULT_SCAN(:show_streamlits_query_id))
    UNION ALL
    SELECT * FROM TABLE(RESULT_SCAN(:show_apps_installed_query_id))
    UNION ALL
    SELECT * FROM TABLE(RESULT_SCAN(:show_app_packages_query_id))
  );

  RETURN TABLE(final_result);
END;
