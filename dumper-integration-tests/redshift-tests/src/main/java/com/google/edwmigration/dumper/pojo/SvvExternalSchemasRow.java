package com.google.edwmigration.dumper.pojo;

import static com.google.edwmigration.dumper.jdbc.JdbcUtil.getIntNotNull;
import static com.google.edwmigration.dumper.jdbc.JdbcUtil.getStringNotNull;
import static java.lang.System.lineSeparator;

import com.google.auto.value.AutoValue;
import com.google.edwmigration.dumper.csv.CsvUtil;
import java.sql.ResultSet;
import java.sql.SQLException;

@AutoValue
public abstract class SvvExternalSchemasRow {

  public static SvvExternalSchemasRow create(
      int esoid,
      int eskind,
      String schemaname,
      int esowner,
      String databasename,
      String esoptions
  ) {
    return new AutoValue_SvvExternalSchemasRow(esoid, eskind, schemaname, esowner, databasename, esoptions);
  }

  public static SvvExternalSchemasRow create(ResultSet rs) throws SQLException {
    return SvvExternalSchemasRow.create(
        getIntNotNull(rs, "esoid"),
        getIntNotNull(rs, "eskind"),
        getStringNotNull(rs, "schemaname"),
        getIntNotNull(rs, "esowner"),
        getStringNotNull(rs, "databasename"),
        getStringNotNull(rs, "esoptions")
    );
  }

  public static SvvExternalSchemasRow create(String[] csvLine) {
    return new AutoValue_SvvExternalSchemasRow(
        CsvUtil.getIntNotNull(csvLine[0]),
        CsvUtil.getIntNotNull(csvLine[1]),
        CsvUtil.getStringNotNull(csvLine[2]),
        CsvUtil.getIntNotNull(csvLine[3]),
        CsvUtil.getStringNotNull(csvLine[4]),
        CsvUtil.getStringNotNull(csvLine[5])
    );
  }

  public abstract int esoid();

  public abstract int eskind();

  public abstract String schemaname();

  public abstract int esowner();

  public abstract String databasename();

  public abstract String esoptions();

  @Override
  public String toString() {
    return "esoid="
        + esoid()
        + ", eskind="
        + eskind()
        + ", schemaname="
        + schemaname()
        + ", esowner="
        + esowner()
        + ", databasename="
        + databasename()
        + ", esoptions="
        + esoptions()
        + lineSeparator();
  }

}
