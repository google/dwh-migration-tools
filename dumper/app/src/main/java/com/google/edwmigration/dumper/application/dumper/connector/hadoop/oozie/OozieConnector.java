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
package com.google.edwmigration.dumper.application.dumper.connector.hadoop.oozie;

import com.google.auto.service.AutoService;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnector;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.utils.ArchiveNameUtil;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import java.time.Clock;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.oozie.client.XOozieClient;

@AutoService(Connector.class)
@Description("Dumps Jobs history from Oozie.")
@RespectsInput(order = 100, arg = ConnectorArguments.OPT_URI, description = "Oozie URL.")
@RespectsInput(
    order = 200,
    arg = ConnectorArguments.OPT_USER,
    description = "The username for Oozie with BASIC authentication")
@RespectsInput(
    order = 300,
    arg = ConnectorArguments.OPT_PASSWORD,
    description = "The password for Oozie with BASIC authentication",
    required = "If not specified as an argument, will use a secure prompt")
public class OozieConnector extends AbstractConnector implements MetadataConnector {
  private static final String FORMAT_NAME = "oozie.dump.zip";

  private static final int MAX_QUARTER_DAY = 93;

  public OozieConnector() {
    super("oozie");
  }

  @Nonnull
  @Override
  public String getDefaultFileName(boolean isAssessment, Clock clock) {
    return ArchiveNameUtil.getFileNameWithTimestamp(getName(), clock);
  }

  @Override
  public void addTasksTo(@Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws Exception {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));
    out.add(new OozieInfoTask());
    out.add(new OozieServersTask());
    out.add(new OozieWorkflowJobsTask(MAX_QUARTER_DAY));
    out.add(new OozieCoordinatorJobsTask(MAX_QUARTER_DAY));
  }

  @Nonnull
  @Override
  public Handle open(@Nonnull ConnectorArguments arguments) throws Exception {
    String oozieUrl = arguments.getUri();
    String user = arguments.getUser();
    String password = null;
    if (user != null) {
      password = arguments.getPasswordOrPrompt();
    }
    XOozieClient xOozieClient = OozieClientFactory.createXOozieClient(oozieUrl, user, password);
    return new OozieHandle(xOozieClient);
  }
}
