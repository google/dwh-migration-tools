package com.google.edwmigration.validation.application.validator;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** @author nehanene */
@RunWith(JUnit4.class)
public class ValidationConnectionTest {

  private static final String DEFAULT_HOST = "default_host";
  private static final int DEFAULT_PORT = 1234;
  private static final String DEFAULT_DRIVER_CLASS = "default.jdbc.Driver";

  // Helper method to create a ValidationConnection instance for testing
  private ValidationConnection createConnection(
      String connectionType,
      String driver,
      String host,
      String port,
      String user,
      String password,
      String database,
      String projectId,
      String serviceAccount,
      String uri,
      String jdbcDriverClass) {
    return new ValidationConnection(
        connectionType,
        driver,
        host,
        port,
        user,
        password,
        database,
        projectId,
        serviceAccount,
        uri,
        jdbcDriverClass
    );
  }

  // --- Constructor and basic Getters Tests ---

  @Test
  public void testConstructorAndGetters_AllNonNull() {
    ValidationConnection connection = createConnection(
        "TEST_TYPE",
        "path/to/driver.jar",
        "test_host",
        "5432",
        "test_user",
        "test_pass",
        "test_db",
        "test_project",
        "test_sa",
        "jdbc:uri",
        "com.example.JdbcDriver"
    );

    assertEquals("TEST_TYPE", connection.getConnectionType());
    assertEquals("test_host", connection.getHost());
    assertEquals(Integer.valueOf(5432), connection.getPort());
    assertEquals("test_user", connection.getUser());
    assertEquals("test_pass", connection.getPassword()); // Test stored password directly
    assertEquals("test_db", connection.getDatabase());
    assertEquals("test_project", connection.getProjectId());
    assertEquals("test_sa", connection.getServiceAccount());
    assertEquals("jdbc:uri", connection.getUri());
    assertEquals("com.example.JdbcDriver", connection.getDriverClass(""));
    assertEquals(Arrays.asList("path/to/driver.jar"), connection.getDriverPaths());
  }

  @Test
  public void testConstructorAndGetters_SomeNull() {
    ValidationConnection connection = createConnection(
        null, // connectionType
        "path/to/driver.jar",
        null, // host
        "8080", // port
        null, // user
        null, // password (will trigger PasswordReader, not testing here)
        null, // database
        null, // projectId
        null, // serviceAccount
        null, // uri
        null // jdbcDriverClass
    );

    assertNull(connection.getConnectionType());
    assertNull(connection.getHost());
    assertEquals(Integer.valueOf(8080), connection.getPort());
    assertNull(connection.getUser());

    assertNull(connection.getDatabase());
    assertNull(connection.getProjectId());
    assertNull(connection.getServiceAccount());
    assertNull(connection.getUri());
    assertNull(connection.getDriverClass(null)); // Test getDriverClass with null default
    assertEquals(Arrays.asList("path/to/driver.jar"), connection.getDriverPaths());
  }

  // --- GetHost(defaultHost) Tests ---

  @Test
  public void testGetHost_WithHostValue() {
    ValidationConnection connection = createConnection(
        "type", "driver", "myhost.com", "1234", "user", "pass", "db", "proj", "sa", "uri", "driverClass"
    );
    assertEquals("myhost.com", connection.getHost(DEFAULT_HOST));
  }

  @Test
  public void testGetHost_WithNullHostValue() {
    ValidationConnection connection = createConnection(
        "type", "driver", null, "1234", "user", "pass", "db", "proj", "sa", "uri", "driverClass"
    );
    assertEquals(DEFAULT_HOST, connection.getHost(DEFAULT_HOST));
  }

  // --- Tests for getPort() ---

  @Test
  public void testGetPort_ValidPortString() {
    ValidationConnection connection = createConnection(
        "type", "driver", "host", "54321", "user", "pass", "db", "proj", "sa", "uri", "driverClass"
    );
    assertEquals(Integer.valueOf(54321), connection.getPort());
  }

  @Test(expected = NumberFormatException.class)
  public void testGetPort_InvalidPortStringThrowsNumberFormatException() {
    ValidationConnection connection = createConnection(
        "type", "driver", "host", "not_a_number", "user", "pass", "db", "proj", "sa", "uri", "driverClass"
    );
    connection.getPort();
  }

  @Test
  public void testGetPort_NullPortStringReturnsNull() {
    ValidationConnection connection = createConnection(
        "type", "driver", "host", null, "user", "pass", "db", "proj", "sa", "uri", "driverClass"
    );
    assertNull(connection.getPort()); // Explicitly returns null if port is null
  }

  // --- Tests for getPort(int defaultPort) ---

  @Test
  public void testGetPortWithDefault_ValidPortString() {
    ValidationConnection connection = createConnection(
        "type", "driver", "host", "54321", "user", "pass", "db", "proj", "sa", "uri", "driverClass"
    );
    assertEquals(54321, connection.getPort(DEFAULT_PORT));
  }

  @Test(expected = NumberFormatException.class)
  public void testGetPortWithDefault_InvalidPortStringThrowsNumberFormatException() {
    ValidationConnection connection = createConnection(
        "type", "driver", "host", "not_a_number", "user", "pass", "db", "proj", "sa", "uri", "driverClass"
    );
    connection.getPort(DEFAULT_PORT);
  }

  @Test
  public void testGetPortWithDefault_NullPortStringReturnsDefault() {
    ValidationConnection connection = createConnection(
        "type", "driver", "host", null, "user", "pass", "db", "proj", "sa", "uri", "driverClass"
    );
    assertEquals(DEFAULT_PORT, connection.getPort(DEFAULT_PORT));
  }


  // --- GetPassword Tests ---
  @Test
  public void testGetPassword_WithProvidedPassword() {
    ValidationConnection connection = createConnection(
        "type", "driver", "host", "1234", "user", "mysecret", "db", "proj", "sa", "uri", "driverClass"
    );
    assertEquals("mysecret", connection.getPassword());
  }

  // --- GetDriverPaths Tests ---

  @Test
  public void testGetDriverPaths_SingleDriver() {
    ValidationConnection connection = createConnection(
        "type", "path/to/driver.jar", "host", "1234", "user", "pass", "db", "proj", "sa", "uri", "driverClass"
    );
    List<String> expected = Arrays.asList("path/to/driver.jar");
    assertEquals(expected, connection.getDriverPaths());
  }

  @Test
  public void testGetDriverPaths_MultipleDrivers() {
    ValidationConnection connection = createConnection(
        "type", "driver1.jar,driver2.jar,driver3.jar", "host", "1234", "user", "pass", "db", "proj", "sa", "uri", "driverClass"
    );
    List<String> expected = Arrays.asList("driver1.jar", "driver2.jar", "driver3.jar");
    assertEquals(expected, connection.getDriverPaths());
  }

  @Test
  public void testGetDriverPaths_MultipleDriversWithSpacesAndEmptyParts() {
    ValidationConnection connection = createConnection(
        "type", " driver1.jar , , driver2.jar ", "host", "1234", "user", "pass", "db", "proj", "sa", "uri", "driverClass"
    );
    List<String> expected = Arrays.asList("driver1.jar", "driver2.jar");
    assertEquals(expected, connection.getDriverPaths());
  }

  @Test
  public void testGetDriverPaths_EmptyDriverString() {
    ValidationConnection connection = createConnection(
        "type", "", "host", "1234", "user", "pass", "db", "proj", "sa", "uri", "driverClass"
    );
    assertTrue(connection.getDriverPaths().isEmpty());
  }

  @Test
  public void testGetDriverPaths_NullDriverString() {
    ValidationConnection connection = createConnection(
        "type", null, "host", "1234", "user", "pass", "db", "proj", "sa", "uri", "driverClass"
    );
    assertNull(connection.getDriverPaths());
  }


  // --- GetDriverClass(defaultDriverClass) Tests ---

  @Test
  public void testGetDriverClass_WithDriverClassValue() {
    ValidationConnection connection = createConnection(
        "type", "driver", "host", "1234", "user", "pass", "db", "proj", "sa", "uri", "my.custom.Driver"
    );
    assertEquals("my.custom.Driver", connection.getDriverClass(DEFAULT_DRIVER_CLASS));
  }

  @Test
  public void testGetDriverClass_WithNullDriverClassValue() {
    ValidationConnection connection = createConnection(
        "type", "driver", "host", "1234", "user", "pass", "db", "proj", "sa", "uri", null
    );
    assertEquals(DEFAULT_DRIVER_CLASS, connection.getDriverClass(DEFAULT_DRIVER_CLASS));
  }

  // --- toString() Test ---

  @Test
  public void testToString_WithNullValues() {
    ValidationConnection connection = createConnection(
        null, // connectionType
        "", // driver (empty, results in empty list for getDriverPaths)
        null, // host
        "8080", // port
        null, // user
        "password_value", // password (will not be in toString anyway)
        null, // database
        null, // projectId
        null, // serviceAccount
        null, // uri (not included in toString)
        null // jdbcDriverClass (not included in toString)
    );

    String toStringResult = connection.toString();
    assertNotNull(toStringResult);
    assertFalse(toStringResult.contains("connectionType="));
    assertFalse(toStringResult.contains("host="));
    assertFalse(toStringResult.contains("user="));
    assertFalse(toStringResult.contains("database="));
    assertFalse(toStringResult.contains("projectId="));
    assertFalse(toStringResult.contains("serviceAccount="));
    assertFalse(toStringResult.contains("password")); // Ensure password is never there

    assertTrue(toStringResult.contains("port=8080"));
    assertTrue(toStringResult.contains("driver=[]")); // Empty driver list should be present
  }
}