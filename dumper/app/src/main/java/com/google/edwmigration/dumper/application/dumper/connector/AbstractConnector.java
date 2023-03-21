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

import com.google.common.base.Preconditions;
import javax.annotation.Nonnull;

/** @author shevek */
public abstract class AbstractConnector implements Connector {

  private final String name;

  public AbstractConnector(@Nonnull String name) {
    this.name = Preconditions.checkNotNull(name, "Name was null.");
  }

  @Override
  public String getName() {
    return name;
  }
}
