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
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.util.List;
import javax.annotation.Nonnull;

/** @author shevek */
public interface Connector {

  // Empty
  public enum DefaultProperties implements ConnectorProperty {}

  @Nonnull
  public String getName();

  @Nonnull
  public String getDefaultFileName(boolean isAssessment);

  @Nonnull
  public void addTasksTo(@Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws Exception;

  @Nonnull
  public Handle open(@Nonnull ConnectorArguments arguments) throws Exception;

  @Nonnull
  public default Class<? extends Enum<? extends ConnectorProperty>> getConnectorProperties() {
    return DefaultProperties.class;
  }
}
