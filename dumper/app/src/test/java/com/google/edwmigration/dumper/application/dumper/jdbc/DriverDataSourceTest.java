/*
 * Copyright 2022-2025 Google LLC
 * Copyright 2013-2021 CompilerWorks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.edwmigration.dumper.application.dumper.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class DriverDataSourceTest {

  @Test
  public void getConnection_withUserAndPassword_passesCredentialsAndTrimsUrl() throws SQLException {
    Driver driver = mock(Driver.class);
    Connection connection = mock(Connection.class);
    when(driver.connect(eq("jdbc:test://localhost"), any(Properties.class))).thenReturn(connection);

    DriverDataSource dataSource =
        new DriverDataSource(driver, "  jdbc:test://localhost  ", "alice", "secret");

    assertSame(driver, dataSource.getDriver());
    assertEquals("jdbc:test://localhost", dataSource.getUrl());
    assertEquals("alice", dataSource.getUsername());
    assertEquals("secret", dataSource.getPassword());
    assertSame(connection, dataSource.getConnection());

    ArgumentCaptor<Properties> propsCaptor = ArgumentCaptor.forClass(Properties.class);
    verify(driver).connect(eq("jdbc:test://localhost"), propsCaptor.capture());
    assertEquals("alice", propsCaptor.getValue().getProperty("user"));
    assertEquals("secret", propsCaptor.getValue().getProperty("password"));
  }

  @Test
  public void getConnection_withProperties_defensivelyCopiesAndOverridesCredentials()
      throws SQLException {
    Driver driver = mock(Driver.class);
    Connection connection = mock(Connection.class);
    when(driver.connect(eq("jdbc:test://localhost"), any(Properties.class))).thenReturn(connection);

    Properties initial = new Properties();
    initial.setProperty("user", "defaultUser");
    initial.setProperty("role", "analyst");
    DriverDataSource dataSource = new DriverDataSource(driver, "jdbc:test://localhost", initial);
    initial.setProperty("user", "mutated");

    assertEquals("defaultUser", dataSource.getUsername());
    assertSame(connection, dataSource.getConnection("overrideUser", "overridePass"));

    ArgumentCaptor<Properties> propsCaptor = ArgumentCaptor.forClass(Properties.class);
    verify(driver).connect(eq("jdbc:test://localhost"), propsCaptor.capture());
    assertEquals("overrideUser", propsCaptor.getValue().getProperty("user"));
    assertEquals("overridePass", propsCaptor.getValue().getProperty("password"));
    assertEquals("analyst", propsCaptor.getValue().getProperty("role"));
  }

  @Test
  public void getConnection_whenDriverReturnsNull_returnsNull() throws SQLException {
    Driver driver = mock(Driver.class);
    when(driver.connect(eq("jdbc:other://localhost"), any(Properties.class))).thenReturn(null);

    DriverDataSource dataSource = new DriverDataSource(driver, "jdbc:other://localhost");

    assertNull(dataSource.getConnection());
  }

  @Test
  public void dataSourceMethods_behaveLikeSpringAbstractDataSource() throws SQLException {
    Driver driver = mock(Driver.class);
    DriverDataSource dataSource = new DriverDataSource(driver, "jdbc:test://localhost");

    assertEquals(0, dataSource.getLoginTimeout());
    assertThrows(UnsupportedOperationException.class, () -> dataSource.setLoginTimeout(10));
    assertThrows(UnsupportedOperationException.class, dataSource::getLogWriter);
    assertThrows(UnsupportedOperationException.class, () -> dataSource.setLogWriter(null));
    assertNotNull(dataSource.getParentLogger());
    assertTrue(dataSource.isWrapperFor(DataSource.class));
    assertTrue(dataSource.isWrapperFor(DriverDataSource.class));
    assertFalse(dataSource.isWrapperFor(Connection.class));
    assertSame(dataSource, dataSource.unwrap(DataSource.class));
    assertThrows(SQLException.class, () -> dataSource.unwrap(Connection.class));
  }

  @Test
  public void resultSetExtractor_extractsValue() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getString(1)).thenReturn("value");
    ResultSetExtractor<String> extractor = r -> r.getString(1);

    assertEquals("value", extractor.extractData(rs));
  }
}
