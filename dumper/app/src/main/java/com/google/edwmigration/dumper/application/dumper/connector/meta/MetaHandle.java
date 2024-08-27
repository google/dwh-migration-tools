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
package com.google.edwmigration.dumper.application.dumper.connector.meta;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.ConnectorRepository;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;

/**
 * The handle to be used in metaconnector. It contains handles and connector arguments for the child
 * connectors that should be initialized during task executions as opposed to the normal process for
 * a single connector, where the handle and connector arguments are initialized before the task
 * execution is started.
 *
 * <p>It is the responsibility of the implementor of the metaconnector to make sure that the handles
 * and connector arguments are set in this meta-handle before the task that uses them is started.
 * Metaconnector takes care of the proper ordering.
 *
 * <p>Otherwise, there are no requirements for how the handle and the connector arguments are set,
 * i.e. they can be set in a single task or in two tasks (possibly executed in parallel), as long as
 * these tasks finish before the task that uses them is started.
 */
public class MetaHandle implements Handle {
  private final ConcurrentMap<String, Handle> handleMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, ConnectorArguments> argumentsMap = new ConcurrentHashMap<>();

  @Nullable
  public Handle getHandleByConnectorName(String connectorName) {
    return handleMap.get(connectorName);
  }

  @Nullable
  public ConnectorArguments getArgumentsByConnectorName(String connectorName) {
    return argumentsMap.get(connectorName);
  }

  public void initializeConnector(String connectorName, ConnectorArguments arguments)
      throws Exception {
    Connector connector = ConnectorRepository.getInstance().getByName(connectorName);
    argumentsMap.put(connectorName, arguments);
    handleMap.put(connectorName, connector.open(arguments));
  }

  @Override
  public void close() throws IOException {
    for (Map.Entry<String, Handle> entry : handleMap.entrySet()) {
      entry.getValue().close();
    }
  }
}
