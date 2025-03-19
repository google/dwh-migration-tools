package com.google.edwmigration.dbsync.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import com.google.edwmigration.dbsync.test.RsyncTestRunner;
import com.google.edwmigration.dbsync.test.RsyncTestRunner.Flag;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteSource;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.Arrays;
import javax.sql.DataSource;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

public class JdbcByteSourceTest {
  private static final Logger logger = LoggerFactory.getLogger(JdbcByteSourceTest.class);

  private static final String URI = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
  private static final String QUERY =  "select * from t order by t0";

  @Test
  public void testJdbcByteSource() throws Exception {
    DataSource ds = new SimpleDriverDataSource(new org.h2.Driver(), URI);
    JdbcTemplate template = new JdbcTemplate(ds);
    template.execute("create table t (t0 int, t1 int)");
    template.execute("insert into t values (1, 1), (2, 2), (3, 3), (4, 4)");

    DEBUG:
    if (false) {
      StringWriter writer = new StringWriter();
      template.query("select * from t", (ResultSetExtractor<? extends Void>) rs -> {
        try (CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT)) {
          printer.printRecords(rs, true);
          return null;
        } catch (IOException | SQLException e) {
          throw new DataRetrievalFailureException("Failed to format ResultSet", e);
        }
      });
      logger.info("Table contains\n" + writer);
    }

    DUMMY:
    if (false) {
      JdbcByteSource source = new JdbcByteSource(new DummyEncoder(), ds, QUERY);
      HashCode hc = source.hash(Hashing.sha256());
      logger.info("Dummy hash code is " + hc);
    }

    AVRO:
    {
      JdbcByteSource source = new JdbcByteSource(new AvroEncoder(), ds, QUERY);
      byte[] data = source.read();
      logger.info("Avro data is " + Arrays.toString(data));
      HashCode hc = Hashing.sha256().hashBytes(data);
      logger.info("Avro hash code is " + hc);
    }
  }

  @Test
  public void testJdbcRsync() throws Exception {
    logger.info("Starting up JDBC rsync.");
    DataSource ds = new SimpleDriverDataSource(new org.h2.Driver(), URI);
    JdbcTemplate template = new JdbcTemplate(ds);
    SETUP_DATABASE:
    {
      template.execute("drop table if exists t");
      template.execute("create or replace table t (t0 int, t1 int)");
      for (int i = 0; i < 64 * 1024; i++)
        template.update("insert into t values (?, ?)", i, i);
    }

    ByteSource clientData = new JdbcByteSource(new AvroEncoder(), ds, QUERY);
    ByteSource serverData = RsyncTestRunner.newRandomData(18765);

    RUN_FIRST:
    {
      RsyncTestRunner runner = new RsyncTestRunner("JdbcRsync[1] (random state)", serverData, clientData);
      // runner.setFlags(Flag.values());
      // runner.setFlags(Flag.PrintInstructions);
      byte[] serverDataNew = runner.run();
      serverData = ByteSource.wrap(serverDataNew);

      serverDataNew[123] = 42;  // corrupt the server data
    }

    MODIFY_CLIENT:
    {
      template.update("delete from t where t0 between 123 and 456");
      template.update("update t set t1 = t1 + 2 where t0 between 512 and 516");
    }

    RUN_AGAIN:
    {
      RsyncTestRunner runner = new RsyncTestRunner("JdbcRsync[2] (incremental update)", serverData, clientData);
      runner.setFlags(Flag.PrintInstructions);
      runner.run();
    }
  }

}