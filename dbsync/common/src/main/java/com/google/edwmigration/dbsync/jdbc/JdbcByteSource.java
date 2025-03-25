package com.google.edwmigration.dbsync.jdbc;

import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcByteSource extends ByteSource {
  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(JdbcByteSource.class);

  private class JdbcThread extends Thread {
    private final OutputStream out;
    private final AtomicReference<Throwable> throwable = new AtomicReference<>();

    public JdbcThread(OutputStream out) {
      this.out = out;
      setDaemon(true);
    }

    @Override
    public void run() {
      try (OutputStream out = this.out;
          Connection connection = source.getConnection();
          Statement statement = connection.createStatement();
          ResultSet rs = statement.executeQuery(query)) {
        encoder.encodeTo(out, rs);
      } catch (IOException | SQLException | RuntimeException e) {
        throwable.set(e);
      }
    }
  }

  private final JdbcEncoder encoder;
  private final DataSource source;
  private final String query;

  public JdbcByteSource(JdbcEncoder encoder, DataSource source, String query) {
    this.encoder = Objects.requireNonNull(encoder);
    this.source = Objects.requireNonNull(source);
    this.query = Objects.requireNonNull(query);
  }

  @Override
  public InputStream openStream() throws IOException {
    PipedOutputStream out = new PipedOutputStream();
    JdbcThread thread = new JdbcThread(out);
    PipedInputStream in = new PipedInputStream(out, 1024 * 1024) {
      @Override
      public void close() throws IOException {
        super.close();
        Throwable t = thread.throwable.get();
        if (t != null) {
          Throwables.throwIfInstanceOf(t, IOException.class);
          throw new IOException(t);
        }
      }

      @Override
      public String toString() {
        return "PipedInputStream(" + query + ")";
      }
    };
    thread.start();
    return in;
  }
}
