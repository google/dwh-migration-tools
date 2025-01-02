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
package com.google.edwmigration.dumper.application.dumper.connector.test;

import com.google.auto.service.AutoService;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnector;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.util.List;
import javax.annotation.Nonnull;

@AutoService({Connector.class, MetadataConnector.class})
public class TestConnector extends AbstractConnector implements MetadataConnector {
  private static final Handle DUMMY_HANDLE = () -> {};

  public TestConnector() {
    super("test");
  }

  public enum TestConnectorProperty implements ConnectorProperty {
    TEST_PROPERTY("test.property", "This is a test property");

    private final String name;
    private final String description;

    TestConnectorProperty(String name, String description) {
      this.name = "test." + name;
      this.description = description;
    }

    @Nonnull
    public String getName() {
      return name;
    }

    @Nonnull
    public String getDescription() {
      return description;
    }
  }

  @Nonnull
  @Override
  public Class<? extends Enum<? extends ConnectorProperty>> getConnectorProperties() {
    return TestConnectorProperty.class;
  }

  @Override
  public void addTasksTo(
      @Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments) {
    // do nothing in tests
  }

  @Nonnull
  @Override
  public Handle open(@Nonnull ConnectorArguments arguments) {
    return DUMMY_HANDLE;
  }
}
