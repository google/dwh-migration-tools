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
package com.google.edwmigration.dumper.application.dumper.connector;

import static java.util.Arrays.stream;

import com.google.common.base.Preconditions;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import javax.annotation.Nonnull;

/** @author shevek */
public abstract class AbstractConnector implements Connector {

  private final String name;

  public AbstractConnector(@Nonnull String name) {
    this.name = Preconditions.checkNotNull(name, "Name was null.");
  }

  @Nonnull
  public String getDescription() {
    Description description = getClass().getAnnotation(Description.class);
    if (description != null) {
      return description.value();
    }
    return "";
  }

  @Nonnull
  @Override
  public String getName() {
    return name;
  }

  /**
   * Get the list of this Connector's properties, wrapped inside an enum class.
   *
   * <p>Overriding this method changes the behavior of {@link
   * AbstractConnector#getPropertyConstants}. If you override it, don't override this method - it
   * will have no effect.
   *
   * @return An enum where the values represent the properties supported by this connector.
   */
  @Nonnull
  protected Class<? extends Enum<? extends ConnectorProperty>> getConnectorProperties() {
    return DefaultProperties.class;
  }

  /**
   * Get the list of this Connector's properties.
   *
   * <p>You may override this method to set the ConnectorProperties of this Connector.
   * Implementations should behave like a getter of a constant field. Overriding this method is an
   * alternative to overriding {@link AbstractConnector#getConnectorProperties}. When both are
   * overriden, this will take precedence.
   *
   * @return An array of the properties supported by this connector.
   */
  @Nonnull
  @Override
  public Iterable<ConnectorProperty> getPropertyConstants() {
    Enum<? extends ConnectorProperty>[] constants = getConnectorProperties().getEnumConstants();
    return () -> stream(constants).map(property -> (ConnectorProperty) property).iterator();
  }
}
