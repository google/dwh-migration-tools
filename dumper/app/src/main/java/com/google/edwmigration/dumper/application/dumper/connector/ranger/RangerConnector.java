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
package com.google.edwmigration.dumper.application.dumper.connector.ranger;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnector;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerPageIterator.Page;
import com.google.edwmigration.dumper.application.dumper.handle.AbstractHandle;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat.PoliciesFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat.ServicesFormat;
import com.google.errorprone.annotations.ForOverride;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RespectsInput(
    order = 100,
    arg = ConnectorArguments.OPT_HOST,
    description = "The hostname of the Ranger server.",
    defaultValue = ConnectorArguments.OPT_HOST_DEFAULT)
@RespectsInput(
    order = 101,
    arg = ConnectorArguments.OPT_PORT,
    description = "The port of the Ranger server.",
    defaultValue = ConnectorArguments.OPT_RANGER_PORT_DEFAULT)
@AutoService({Connector.class})
@Description("Dumps services and policies from Apache Ranger.")
public class RangerConnector extends AbstractConnector {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(RangerConnector.class);

  public RangerConnector() {
    super("ranger");
  }

  @Nonnull
  @Override
  public String getDefaultFileName(boolean isAssessment) {
    return "dwh-migration-" + getName() + "-data.zip";
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, @Nonnull ConnectorArguments arguments) {
    out.add(new DumpServicesTask());
    out.add(new DumpPoliciesTask());
  }

  @Nonnull
  @Override
  public Handle open(@Nonnull ConnectorArguments arguments) throws Exception {
    return new RangerClientHandle(
        new RangerClient(
            // TODO(aleofreddi): check if we need to handle SSL or Kerberos here.
            "http://" + arguments.getHost() + ":" + arguments.getPort(),
            /* authType= */ "simple", // Use basic auth instead of Kerberos.
            arguments.getUser(),
            arguments.getPasswordOrPrompt(),
            null),
        arguments.getRangerPageSizeDefault());
  }

  static class DumpServicesTask extends AbstractRangerTask<RangerService> {

    DumpServicesTask() {
      super(ServicesFormat.ZIP_ENTRY_NAME);
    }

    protected Iterator<RangerService> dataIterator(
        @Nonnull Writer writer, @Nonnull RangerClientHandle handle) {
      return new RangerPageIterator<>(
          page -> {
            try {
              return handle.rangerClient.findServices(toParameters(page));
            } catch (RangerServiceException e) {
              throw new RuntimeException("Failed to fetch Ranger services", e);
            }
          },
          handle.pageSize);
    }

    @Override
    protected String toCallDescription() {
      return "Ranger services";
    }
  }

  static class DumpPoliciesTask extends AbstractRangerTask<RangerPolicy> {

    DumpPoliciesTask() {
      super(PoliciesFormat.ZIP_ENTRY_NAME);
    }

    @Override
    protected Iterator<RangerPolicy> dataIterator(
        @Nonnull Writer writer, @Nonnull RangerClientHandle handle) {
      return new RangerPageIterator<>(
          page -> {
            try {
              return handle.rangerClient.findPolicies(toParameters(page));
            } catch (RangerServiceException e) {
              throw new RuntimeException("Failed to fetch Ranger policies", e);
            }
          },
          handle.pageSize);
    }

    @Override
    protected String toCallDescription() {
      return "Ranger policies";
    }
  }

  private abstract static class AbstractRangerTask<T> extends AbstractTask<Void> {

    public AbstractRangerTask(String targetPath) {
      super(targetPath);
    }

    @ForOverride
    protected abstract Iterator<T> dataIterator(
        @Nonnull Writer writer, @Nonnull RangerClientHandle handle) throws Exception;

    @Override
    protected Void doRun(TaskRunContext context, ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      RangerClientHandle rangerClientHandler = (RangerClientHandle) handle;
      LOG.info("Writing to '{}' -> '{}'", getTargetPath(), sink);
      try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
        for (Iterator<T> iterator = dataIterator(writer, rangerClientHandler);
            iterator.hasNext(); ) {
          String json = RangerDumpFormat.MAPPER.writeValueAsString(iterator.next());
          writer.write(json);
          writer.write('\n');
        }
      }
      return null;
    }

    @ForOverride
    protected abstract String toCallDescription();

    protected String describeSourceData() {
      return "from " + toCallDescription();
    }

    protected ImmutableMap<String, String> toParameters(Page page) {
      return ImmutableMap.of(
          "startIndex", String.valueOf(page.offset()),
          "pageSize", String.valueOf(page.limit()));
    }
  }

  static class RangerClientHandle extends AbstractHandle {

    public final RangerClient rangerClient;

    public final int pageSize;

    RangerClientHandle(RangerClient rangerClient, int pageSize) {
      this.rangerClient = rangerClient;
      this.pageSize = pageSize;
    }
  }
}