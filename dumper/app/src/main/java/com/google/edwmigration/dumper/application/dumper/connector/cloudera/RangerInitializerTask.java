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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.ClouderaConnector.ClouderaConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.meta.MetaHandle;
import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerConnector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RangerInitializerTask extends AbstractTask<Void> {

  public RangerInitializerTask() {
    super("ranger-initializer.txt", /* createTarget= */ false);
  }

  @CheckForNull
  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    MetaHandle metaHandle = (MetaHandle) handle;
    ConnectorArguments childConnectorArguments =
        tunnelPropertiesToChildConnector(context.getArguments());
    metaHandle.initializeConnector(RangerConnector.NAME, childConnectorArguments);
    return null;
  }

  private ConnectorArguments tunnelPropertiesToChildConnector(
      ConnectorArguments metaconnectorArguments) throws IOException {
    ImmutableList.Builder<String> argumentsBuilder = ImmutableList.builder();
    argumentsBuilder.add("--connector").add(RangerConnector.NAME);
    @Nullable String user = metaconnectorArguments.getDefinition(ClouderaConnectorProperty.USER);
    if (user != null) {
      argumentsBuilder.add("--user").add(user);
    }
    @Nullable
    String password = metaconnectorArguments.getDefinition(ClouderaConnectorProperty.PASSWORD);
    if (password != null) {
      argumentsBuilder.add("--password").add(password);
    }
    return new ConnectorArguments(argumentsBuilder.build().toArray(new String[0]));
  }

  @Override
  public String toString() {
    return "Connecting to Ranger";
  }
}
