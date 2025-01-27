/*
 * Copyright 2022-2024 Google LLC
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
package com.google.edwmigration.validation.application.validator.connector;

import com.google.edwmigration.validation.application.validator.ValidationArguments;
import java.sql.Driver;
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

  protected abstract ClassLoader newDriverParentClassLoader();

  protected abstract ClassLoader newDriverClassLoader(
      @Nonnull ClassLoader parentClassLoader, @CheckForNull List<String> driverPaths);

  protected abstract Class<?> newDriverClass(
      @Nonnull ClassLoader driverClassLoader, @Nonnull String driverClassName);

  protected abstract Driver newDriver(
      @CheckForNull List<String> driverPaths, @Nonnull String... driverClassNames);

  protected abstract SimpleDriverDataSource newSimpleDataSource(
      @Nonnull Driver driver, @Nonnull String url, @Nonnull ValidationArguments arguments);
}
