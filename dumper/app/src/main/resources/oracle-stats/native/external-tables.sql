SELECT
  A.con_id,
  A.owner,
  A.table_name,
  A.type_owner,
  A.type_name,
  A.default_directory_owner,
  A.default_directory_name
FROM
  cdb_external_tables A
