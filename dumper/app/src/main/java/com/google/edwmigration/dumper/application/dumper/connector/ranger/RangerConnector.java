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

import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_RANGER_PORT_DEFAULT;
import static java.lang.Integer.parseInt;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnector;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.RangerInitializerTask;
import com.google.edwmigration.dumper.application.dumper.connector.meta.ChildConnector;
import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerClient.RangerException;
import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerPageIterator.Page;
import com.google.edwmigration.dumper.application.dumper.handle.AbstractHandle;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.application.dumper.utils.ArchiveNameUtil;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat.Group;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat.GroupsFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat.PoliciesFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat.Policy;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat.Role;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat.RolesFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat.Service;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat.ServicesFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat.User;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat.UsersFormat;
import com.google.errorprone.annotations.ForOverride;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
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
    defaultValue = OPT_RANGER_PORT_DEFAULT)
@AutoService({Connector.class})
@Description("Dumps services and policies from Apache Ranger.")
public class RangerConnector extends AbstractConnector implements ChildConnector {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(RangerConnector.class);

  public static final String NAME = "ranger";

  public RangerConnector() {
    super(NAME);
  }

  @Nonnull
  @Override
  public String getDefaultFileName(boolean isAssessment, Clock clock) {
    return ArchiveNameUtil.getFileName(getName());
  }

  @Override
  public void addTasksTo(
      @Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments) {
    out.add(new DumpGroupsTask());
    out.add(new DumpPoliciesTask());
    out.add(new DumpRolesTask());
    out.add(new DumpServicesTask());
    out.add(new DumpUsersTask());
  }

  @Nonnull
  @Override
  public Handle open(@Nonnull ConnectorArguments arguments) throws Exception {
    // TODO: handle SSL or Kerberos.
    URI apiUrl =
        URI.create(
            "http://"
                + arguments.getHostOrDefault()
                + ":"
                + arguments.getPort(parseInt(OPT_RANGER_PORT_DEFAULT)));
    String password = arguments.getPasswordOrPrompt();
    return new RangerClientHandle(
        new RangerClient(apiUrl, arguments.getUser(), password),
        arguments.getRangerPageSizeDefault());
  }

  @Nonnull
  @Override
  public Optional<Task<?>> createInitializerTask() {
    return Optional.of(new RangerInitializerTask());
  }

  static class DumpUsersTask extends AbstractRangerTask<User> {

    DumpUsersTask() {
      super(UsersFormat.ZIP_ENTRY_NAME);
    }

    @Override
    protected Iterator<User> dataIterator(@Nonnull RangerClientHandle handle) {
      return new RangerPageIterator<>(
          page -> {
            try {
              return handle.rangerClient.findUsers(toParameters(page));
            } catch (RangerException e) {
              throw new RuntimeException("Failed to fetch Ranger users", e);
            }
          },
          handle.pageSize);
    }

    @Override
    protected String toCallDescription() {
      return "Ranger users";
    }
  }

  static class DumpGroupsTask extends AbstractRangerTask<Group> {

    DumpGroupsTask() {
      super(GroupsFormat.ZIP_ENTRY_NAME);
    }

    @Override
    protected Iterator<Group> dataIterator(@Nonnull RangerClientHandle handle) {
      return new RangerPageIterator<>(
          page -> {
            try {
              return handle.rangerClient.findGroups(toParameters(page));
            } catch (RangerException e) {
              throw new RuntimeException("Failed to fetch Ranger groups", e);
            }
          },
          handle.pageSize);
    }

    @Override
    protected String toCallDescription() {
      return "Ranger groups";
    }
  }

  static class DumpRolesTask extends AbstractRangerTask<Role> {

    DumpRolesTask() {
      super(RolesFormat.ZIP_ENTRY_NAME);
    }

    @Override
    protected Iterator<Role> dataIterator(@Nonnull RangerClientHandle handle) {
      return new RangerPageIterator<>(
          page -> {
            try {
              return handle.rangerClient.findRoles(toParameters(page));
            } catch (RangerException e) {
              throw new RuntimeException("Failed to fetch Ranger roles", e);
            }
          },
          handle.pageSize);
    }

    @Override
    protected String toCallDescription() {
      return "Ranger roles";
    }
  }

  static class DumpServicesTask extends AbstractRangerTask<Service> {

    DumpServicesTask() {
      super(ServicesFormat.ZIP_ENTRY_NAME);
    }

    protected Iterator<Service> dataIterator(@Nonnull RangerClientHandle handle) {
      return new RangerPageIterator<>(
          page -> {
            try {
              return handle.rangerClient.findServices(toParameters(page));
            } catch (RangerException e) {
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

  static class DumpPoliciesTask extends AbstractRangerTask<Policy> {

    DumpPoliciesTask() {
      super(PoliciesFormat.ZIP_ENTRY_NAME);
    }

    @Override
    protected Iterator<Policy> dataIterator(@Nonnull RangerClientHandle handle) {
      return new RangerPageIterator<>(
          page -> {
            try {
              return handle.rangerClient.findPolicies(toParameters(page));
            } catch (RangerException e) {
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
    protected abstract Iterator<T> dataIterator(@Nonnull RangerClientHandle handle);

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      RangerClientHandle rangerClientHandler = (RangerClientHandle) handle;
      LOG.info("Writing to '{}' -> '{}'", getTargetPath(), sink);
      try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
        for (Iterator<T> iterator = dataIterator(rangerClientHandler); iterator.hasNext(); ) {
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

    protected ImmutableMap<String, String> toParameters(@Nonnull Page page) {
      return ImmutableMap.of(
          "startIndex", String.valueOf(page.offset()),
          "pageSize", String.valueOf(page.limit()));
    }
  }

  static class RangerClientHandle extends AbstractHandle {

    public final RangerClient rangerClient;

    public final int pageSize;

    RangerClientHandle(@Nonnull RangerClient rangerClient, int pageSize) {
      this.rangerClient = rangerClient;
      this.pageSize = pageSize;
    }
  }
}
