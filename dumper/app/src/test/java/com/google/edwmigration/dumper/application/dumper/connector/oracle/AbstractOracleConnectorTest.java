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
  private static final String EXAMPLE_PASSWORD = "p4ssw0rd";
  private static final String EXAMPLE_USER = "user123";

  private final ConnectorArguments arguments = mock(ConnectorArguments.class);
  private final AbstractOracleConnector connector = new OracleMetadataConnector();

  @Test
  public void testValid() {
    when(arguments.getPasswordOrPrompt()).thenReturn(EXAMPLE_PASSWORD);
    when(arguments.getUserOrFail()).thenReturn(EXAMPLE_USER);

    Properties actual = connector.buildProperties(arguments);

    assertEquals(EXAMPLE_PASSWORD, actual.getProperty("password"));
    assertEquals(EXAMPLE_USER, actual.getProperty("user"));
  }
}
