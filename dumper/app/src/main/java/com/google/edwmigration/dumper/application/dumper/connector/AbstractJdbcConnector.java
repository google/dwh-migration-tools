/*
 * Copyright 2022-2023 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.connector;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.io.File;
import java.net.MalformedURLException;
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

/** @author shevek */
public abstract class AbstractJdbcConnector extends AbstractConnector {

  @SuppressWarnings("UnusedVariable")
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

  /**
   * Creates a new ClassLoader for loading the JDBC Driver.
   *
   * @param parentClassLoader The parent ClassLoader.
   * @param driverPath A comma-separated list of paths to JAR files for inclusion in the new
   *     ClassLoader.
   * @return The JDBC ClassLoader to use to load the Driver.
   * @throws PrivilegedActionException
   * @throws MalformedURLException
   */
  @Nonnull
  private static ClassLoader newDriverClassLoader(
      @Nonnull ClassLoader parentClassLoader, @CheckForNull List<? extends String> driverPaths)
      throws PrivilegedActionException, MalformedURLException {
    if (driverPaths == null || driverPaths.isEmpty()) return parentClassLoader;
    List<URL> urls = new ArrayList<>();
    for (String driverPath : driverPaths) {
      File driverFile = new File(driverPath);
      if (!driverFile.isFile())
        throw new IllegalArgumentException(
            "Not a driver JAR-file: " + driverFile.getAbsolutePath());
      URL u = new URL("jar:" + driverFile.toURI() + "!/");
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
      @CheckForNull List<? extends String> driverPaths, @Nonnull String... driverClassNames)
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
                  "Cannot load " + driverClassName + " from " + driverPaths + ": " + e.getCause());
            else throw e;
          }
        }
        throw new SQLException(
            "Failed to load any of " + Arrays.toString(driverClassNames) + " from " + driverPaths);
      }

      LOG.info("Using JDBC Driver " + driverClass);
      return driverClass.asSubclass(Driver.class).getConstructor().newInstance();
    } catch (ReflectiveOperationException
        | PrivilegedActionException
        | MalformedURLException
        | RuntimeException e) {
      throw new SQLException(
          "Failed to load or instantiate " + driverClass + " from " + driverPaths + ": " + e, e);
    }
  }

  /** Just to put a toString() on it. */
  private static class JdbcDataSource extends SimpleDriverDataSource {

    public JdbcDataSource(Driver driver, String url, String username, String password) {
      super(driver, url, username, password);
    }

    /*
    @Override
    protected Connection getConnectionFromDriver(Properties props) throws SQLException {
        LOG.debug("Connecting", new Exception());
        return super.getConnectionFromDriver(props);
    }
     */
    @Override
    public String toString() {
      return "JdbcDataSource(" + getUrl() + " as " + getUsername() + ")";
    }
  }

  @Nonnull
  protected SimpleDriverDataSource newSimpleDataSource(
      @Nonnull Driver driver, @Nonnull String url, @Nonnull ConnectorArguments arguments)
      throws SQLException, MetadataDumperUsageException {
    if (!driver.acceptsURL(url))
      throw new MetadataDumperUsageException(
          "JDBC driver " + driver + " does not accept URL " + url);
    return new JdbcDataSource(driver, url, arguments.getUser(), arguments.getPassword());
  }
}
