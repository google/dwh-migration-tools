package com.google.edwmigration.dumper.application.dumper.connector.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OracleLogsConnectorTest {

  @Test
  public void getConnectorScope_success() {
    OracleLogsConnector connector = new OracleLogsConnector();
    assertEquals(OracleConnectorScope.LOGS, connector.getConnectorScope());
  }
}
