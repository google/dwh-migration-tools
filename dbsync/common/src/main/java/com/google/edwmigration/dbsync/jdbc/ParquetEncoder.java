package com.google.edwmigration.dbsync.jdbc;

import java.io.OutputStream;
import java.sql.ResultSet;

public class ParquetEncoder implements JdbcEncoder {

  @Override
  public void encodeTo(OutputStream outputStream, ResultSet resultSet) {
    // The implementation of this method is left as an exercise to the reader
    // who has more fortitude for dealing with the eccentricities of the Parquet APIs,
    // which depend on FAR too much of the Hadoop stack, something which I told them
    // was a mistake when they first started, but it took them about 10 years to solidly regret.
  }
}
