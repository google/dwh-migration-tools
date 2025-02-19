package com.google.edwmigration.dbsync.jdbc;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DummyEncoder implements JdbcEncoder {

  @Override
  public void encodeTo(OutputStream out, ResultSet rs) throws IOException, SQLException {
    while (rs.next()) {
      out.write(rs.getRow());
    }
  }
}
