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
  show_notebooks_query_id VARCHAR;
  show_cortex_query_id VARCHAR;
  show_integrations_query_id VARCHAR;
  show_forecasts_query_id VARCHAR;
  show_models_query_id VARCHAR;
  show_data_clean_rooms_query_id VARCHAR;
  show_openflow_query_id VARCHAR;
  show_shares_query_id VARCHAR;
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

  -- Contains notebooks info
  SHOW NOTEBOOKS IN ACCOUNT;

  SELECT 'app', 'notebooks', COUNT(*), 'NOTEBOOKS'
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  show_notebooks_query_id := LAST_QUERY_ID();

  -- Contains cortex AI info
  SHOW CORTEX SEARCH SERVICES IN ACCOUNT;

  SELECT 'app', 'cortex-ai', COUNT(*), 'SERVICES'
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  show_cortex_query_id := LAST_QUERY_ID();

  -- Contains catalog integrations
  SHOW CATALOG INTEGRATIONS;

  SELECT 'app', 'open-catalog', COUNT(*), 'INTEGRATIONS'
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
  WHERE "type" = 'CATALOG';

  show_integrations_query_id := LAST_QUERY_ID();

  -- Contain ML forecasts
  SHOW SNOWFLAKE.ML.FORECAST IN ACCOUNT;

  SELECT 'app', 'snowflake-ml', COUNT(*), 'FORECASTS'
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  show_forecasts_query_id := LAST_QUERY_ID();

  -- Contain models info
  SHOW MODELS IN ACCOUNT;

  SELECT 'app', 'snowflake-ml', COUNT(*), 'MODELS'
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  show_models_query_id := LAST_QUERY_ID();

  -- Contain info about Data Clean Rooms
  SHOW APPLICATIONS LIKE '%CLEAN_ROOM%' IN ACCOUNT;

  SELECT 'app', 'data-clean-rooms', COUNT(*), 'APPLICATIONS'
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  show_data_clean_rooms_query_id := LAST_QUERY_ID();

  SHOW OPENFLOW DATA PLANE INTEGRATIONS;

  SELECT 'data_integration', 'openflow_data_plane', COUNT(*), 'INTEGRATIONS'
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  show_openflow_query_id := LAST_QUERY_ID();

  -- Contains Snowgrid: Data Sharing
  SHOW SHARES IN ACCOUNT;

  SELECT 'app', 'snowgrid', COUNT(*), 'SHARES'
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  show_shares_query_id := LAST_QUERY_ID();

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
    UNION ALL
    SELECT * FROM TABLE(RESULT_SCAN(:show_notebooks_query_id))
    UNION ALL
    SELECT * FROM TABLE(RESULT_SCAN(:show_cortex_query_id))
    UNION ALL
    SELECT * FROM TABLE(RESULT_SCAN(:show_integrations_query_id))
    UNION ALL
    SELECT * FROM TABLE(RESULT_SCAN(:show_forecasts_query_id))
    UNION ALL
    SELECT * FROM TABLE(RESULT_SCAN(:show_models_query_id))
    UNION ALL
    SELECT * FROM TABLE(RESULT_SCAN(:show_data_clean_rooms_query_id))
    UNION ALL
    SELECT * FROM TABLE(RESULT_SCAN(:show_openflow_query_id))
    UNION ALL
    SELECT * FROM TABLE(RESULT_SCAN(:show_shares_query_id))
  );

  RETURN TABLE(final_result);
END;