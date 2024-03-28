package com.google.edwmigration.dumper.application.dumper.connector.oracle;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import java.util.Properties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AbstractOracleConnectorTest {
  // provide mocks only with a username and a password
  // ignore the useFetchSize... property, because it has a default
  private static final String EXPECTED_PASSWORD = "p4ssw0rd";
  private static final String EXPECTED_USER = "user123";

  private final ConnectorArguments arguments = mock(ConnectorArguments.class);
  private final AbstractOracleConnector connector = new OracleMetadataConnector();

  @Test
  public void testValid() {
    when(arguments.getPassword()).thenReturn(EXPECTED_PASSWORD);
    when(arguments.getUserOrFail()).thenReturn(EXPECTED_USER);

    Properties actual = connector.buildProperties(arguments);

    assertEquals(EXPECTED_PASSWORD, actual.getProperty("password"));
    assertEquals(EXPECTED_USER, actual.getProperty("user"));
  }
}
