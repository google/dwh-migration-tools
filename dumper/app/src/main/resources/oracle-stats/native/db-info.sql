
SELECT
  A.dbid "DbId",
  A.name "Name",
  A.db_unique_name "DbUniqueName",
  A.con_id "ConId",
  A.con_dbid "ConDbId",
  A.cdb "Cdb",
  A.database_role "DatabaseRole"
FROM v$database A