package com.google.edwmigration.dbsync.jdbc;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface JdbcEncoder {

  void encodeTo(OutputStream out, ResultSet rs) throws IOException, SQLException;
}
