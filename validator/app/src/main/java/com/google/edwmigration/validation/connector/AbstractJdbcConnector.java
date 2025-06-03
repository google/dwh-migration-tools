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
package com.google.edwmigration.validation.connector;

import com.google.edwmigration.validation.ValidationConnection;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

public abstract class AbstractJdbcConnector extends AbstractConnector {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcConnector.class);

  public AbstractJdbcConnector(@Nonnull String name) {
    super(name);
  }

  @Nonnull
  private static ClassLoader newDriverParentClassLoader() throws PrivilegedActionException {
    return AccessController.doPrivileged(
        new PrivilegedExceptionAction<ClassLoader>() {
          @Override
          public ClassLoader run() throws Exception {
            ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();
            if (parentClassLoader == null) parentClassLoader = getClass().getClassLoader();
            return parentClassLoader;
          }
        });
  }

  @Nonnull
  private static ClassLoader newDriverClassLoader(
      @Nonnull ClassLoader parentClassLoader, @CheckForNull List<String> driverPaths)
      throws PrivilegedActionException, MalformedURLException {
    if (driverPaths == null || driverPaths.isEmpty()) return parentClassLoader;
    List<URL> urls = new ArrayList<>();
    for (String driverPath : driverPaths) {
      URI driverUri = getDriverUri(driverPath);
      URL u = new URL("jar:" + driverUri + "!/");
      urls.add(u);
    }
    final URL[] urls_array = urls.toArray(new URL[0]);
    return AccessController.doPrivileged(
        new PrivilegedExceptionAction<ClassLoader>() {
          @Override
          public ClassLoader run() throws Exception {
            return new URLClassLoader(urls_array, parentClassLoader);
          }
        });
  }

  private static URI getDriverUri(String driverPath) {
    File result = new File(driverPath);
    String absolutePath = result.getAbsolutePath();
    if (!result.exists()) {
      String message = String.format("Jdbc driver does not exist at : '%s'", absolutePath);
      throw new IllegalArgumentException(message);
    } else if (!result.isFile()) {
      String message =
          String.format(
              "The path '%s' is not a regular file. Please provide a path to a driver JAR file.",
              absolutePath);
      throw new IllegalArgumentException(message);
    }
    return result.toURI();
  }

  @Nonnull
  private static Class<?> newDriverClass(
      @Nonnull ClassLoader driverClassLoader, @Nonnull String driverClassName)
      throws PrivilegedActionException {
    return AccessController.doPrivileged(
        new PrivilegedExceptionAction<Class<?>>() {
          @Override
          public Class<?> run() throws Exception {
            return Class.forName(driverClassName, true, driverClassLoader);
          }
        });
  }

  @Nonnull
  protected Driver newDriver(
      @CheckForNull List<String> driverPaths, @Nonnull String... driverClassNames)
      throws SQLException {
    Class<?> driverClass = null;
    try {
      ClassLoader parentClassLoader = newDriverParentClassLoader();
      ClassLoader driverClassLoader = newDriverClassLoader(parentClassLoader, driverPaths);

      CLASS:
      {
        for (String driverClassName : driverClassNames) {
          try {
            driverClass = newDriverClass(driverClassLoader, driverClassName);
            if (driverClass != null) break CLASS;
          } catch (PrivilegedActionException e) {
            if (e.getCause() instanceof ClassNotFoundException)
              LOG.warn(
                  "Cannot load driver class [{}] from path {}: {}",
                  driverClassName,
                  driverPaths,
                  e.getCause());
            else throw e;
          }
        }
        throw new SQLException(
            "Failed to load any driver of "
                + Arrays.toString(driverClassNames)
                + " from path "
                + driverPaths);
      }

      LOG.info("Using JDBC Driver: {}", driverClass);
      return driverClass.asSubclass(Driver.class).getConstructor().newInstance();
    } catch (ReflectiveOperationException
        | PrivilegedActionException
        | MalformedURLException
        | RuntimeException e) {
      throw new SQLException(
          "Failed to load or instantiate jdbc driver: ["
              + driverClass
              + "] from path: "
              + driverPaths,
          e);
    }
  }

  private static class JdbcDataSource extends SimpleDriverDataSource {

    public JdbcDataSource(Driver driver, String url, String username, String password) {
      super(driver, url, username, password);
    }

    @Override
    public String toString() {
      return "JdbcDataSource(" + getUrl() + " as " + getUsername() + ")";
    }
  }

  protected SimpleDriverDataSource newSimpleDataSource(
      @Nonnull Driver driver, @Nonnull String url, @Nonnull ValidationConnection arguments)
      throws SQLException {
    if (!driver.acceptsURL(url))
      throw new SQLException("JDBC driver " + driver + " does not accept URL " + url);
    String password = arguments.getPassword();
    return new JdbcDataSource(driver, url, arguments.getUser(), password);
  }
}
