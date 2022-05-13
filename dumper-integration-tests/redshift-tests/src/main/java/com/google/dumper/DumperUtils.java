/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.dumper;

import static com.google.base.TestConstants.ET_OUTPUT_PATH;

import com.google.common.collect.ImmutableSet;
import com.google.pojo.DbEntity;
import com.google.pojo.PgDatabase;
import com.google.pojo.PgLibrary;
import com.google.pojo.PgNamespace;
import com.google.pojo.PgOperator;
import com.google.pojo.PgTableDef;
import com.google.pojo.PgTables;
import com.google.pojo.PgUser;
import com.google.pojo.PgViews;
import com.google.pojo.SvvExternalColumns;
import com.google.pojo.SvvExternalDatabases;
import com.google.pojo.SvvExternalPartitions;
import com.google.pojo.SvvExternalSchemas;
import com.google.pojo.SvvExternalTables;
import com.google.pojo.SvvTableInfo;
import com.google.pojo.SvvTables;
import com.opencsv.bean.CsvToBeanBuilder;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DumperUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(DumperUtils.class);

  private DumperUtils() {}

  private enum DbEntityEnum {
    PG_USER("pg_user.csv", PgUser.class),
    SVV_TABLES("svv_tables.csv", SvvTables.class),
    SVV_TABLE_INFO("svv_table_info.csv", SvvTableInfo.class),
    SVV_EXTERNAL_SCHEMAS("svv_external_schemas.csv", SvvExternalSchemas.class),
    SVV_EXTERNAL_DATABASES("svv_external_databases.csv", SvvExternalDatabases.class),
    SVV_EXTERNAL_TABLES("svv_external_tables.csv", SvvExternalTables.class),
    SVV_EXTERNAL_COLUMNS("svv_external_columns.csv", SvvExternalColumns.class),
    SVV_EXTERNAL_PARTITIONS("svv_external_partitions.csv", SvvExternalPartitions.class),
    PG_LIBRARY("pg_library.csv", PgLibrary.class),
    PG_DATABASE("pg_database.csv", PgDatabase.class),
    PG_NAMESPACE("pg_namespace.csv", PgNamespace.class),
    PG_OPERATOR("pg_operator.csv", PgOperator.class),
    PG_TABLES("pg_tables.csv", PgTables.class),
    PG_TABLE_DEF_GENERIC("pg_table_def_generic.csv", PgTableDef.class),
    PG_TABLE_DEF_PRIVATE("pg_table_def_private.csv", PgTableDef.class),
    PG_VIEWS_GENERIC("pg_views_generic.csv", PgViews.class),
    PG_VIEWS_PRIVATE("pg_views_private.csv", PgViews.class);

    private final String fileName;
    private final Class<? extends DbEntity> dbEntity;

    DbEntityEnum(String fileName, Class<? extends DbEntity> dbEntity) {
      this.fileName = fileName;
      this.dbEntity = dbEntity;
    }

    public String getFileName() {
      return fileName;
    }

    public Class<? extends DbEntity> getDbEntity() {
      return dbEntity;
    }
  }

  /** Parses csv files into db entity. */
  public static ImmutableSet<DbEntity> parseCsvFile(String fileName) {
    DbEntityEnum dbEntity = getDbEntity(fileName);
    String path = ET_OUTPUT_PATH + fileName;
    List<DbEntity> entries = null;
    try {
      entries =
          new CsvToBeanBuilder(new FileReader(path))
              .withType(dbEntity.getDbEntity())
              .build()
              .parse();
    } catch (FileNotFoundException e) {
      LOGGER.error("Incorrect file path {}", path);
      throw new IllegalArgumentException("Incorrect file path", e);
    }
    return ImmutableSet.copyOf(entries);
  }

  private static DbEntityEnum getDbEntity(String fileName) {
    for (DbEntityEnum entity : DbEntityEnum.values())
      if (entity.getFileName().equals(fileName)) {
        return entity;
      }
    throw new IllegalArgumentException(String.format("File %s is not supported.", fileName));
  }
}
