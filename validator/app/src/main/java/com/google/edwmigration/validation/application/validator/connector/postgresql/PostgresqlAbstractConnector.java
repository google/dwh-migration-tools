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
package com.google.edwmigration.validation.application.validator.connector.postgresql;

import com.google.edwmigration.validation.application.validator.ValidationArguments;
import com.google.edwmigration.validation.application.validator.connector.AbstractJdbcConnector;
import com.google.edwmigration.validation.application.validator.handle.Handle;
import javax.annotation.Nonnull;

public abstract class PostgresqlAbstractConnector extends AbstractJdbcConnector {

  public PostgresqlAbstractConnector(@Nonnull String name) {
    super(name);
  }

  @Nonnull
  @Override
  public Handle open(@Nonnull ValidationArguments arguments) throws Exception {
    return null;
  }
}
