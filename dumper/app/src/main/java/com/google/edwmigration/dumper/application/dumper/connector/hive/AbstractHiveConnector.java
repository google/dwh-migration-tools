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
package com.google.edwmigration.dumper.application.dumper.connector.hive;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteSink;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnector;
import com.google.edwmigration.dumper.application.dumper.handle.AbstractHandle;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.ext.hive.metastore.HiveMetastoreThriftClient;
import com.google.edwmigration.dumper.plugin.ext.jdk.concurrent.ExecutorManager;
import com.google.errorprone.annotations.ForOverride;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.thrift.transport.TTransportException;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RespectsInput(
    order = 100,
    arg = ConnectorArguments.OPT_HOST,
    description = "The hostname of the Hive metastore.",
    defaultValue = ConnectorArguments.OPT_HOST_DEFAULT)
@RespectsInput(
    order = 101,
    arg = ConnectorArguments.OPT_PORT,
    description = "The port of the Hive metastore.",
    defaultValue = ConnectorArguments.OPT_HIVE_METASTORE_PORT_DEFAULT)
@RespectsInput(
    order = 400,
    arg = ConnectorArguments.OPT_HIVE_METASTORE_VERSION,
    description = "The version of the Hive metastore.",
    defaultValue = ConnectorArguments.OPT_HIVE_METASTORE_VERSION_DEFAULT)
@RespectsInput(
    order = 401,
    arg = ConnectorArguments.OPT_HIVE_METASTORE_DUMP_PARTITION_METADATA,
    description =
        "Dump partition metadata; you may wish to disable this for production metastores with a significant number of partitions due to Thrift client performance implications.",
    defaultValue = ConnectorArguments.OPT_HIVE_METASTORE_DUMP_PARTITION_METADATA_DEFAULT)
@RespectsInput(
    order = 500,
    arg = ConnectorArguments.OPT_THREAD_POOL_SIZE,
    description = "The size of the thread pool to use when dumping table metadata.")
public abstract class AbstractHiveConnector extends AbstractConnector {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(AbstractHiveConnector.class);

  /**
   * Each thread in the pool is lazily assigned a new Thrift client upon first use; we don't need to
   * replace them between task executions as the Thrift clients do not preserve state between
   * requests. The important point is that a single Thrift client cannot handle multiple requests
   * simultaneously, by keeping each client thread-local we avoid concurrent use.
   */
  @ThreadSafe
  public static class ThriftClientPool implements AutoCloseable {

    public interface ThriftClientConsumer {
      void accept(HiveMetastoreThriftClient thriftClient) throws Exception;
    }

    @Nonnull private final String name;
    @Nonnull private final ThreadLocal<? extends HiveMetastoreThriftClient> threadLocalThriftClient;
    @Nonnull private final ExecutorManager executorManager;
    @Nonnull private final ExecutorService executorService;
    @Nonnull private final Object lock = new Object();

    @GuardedBy("lock")
    @Nonnull
    private final List<@NonNull HiveMetastoreThriftClient> builtClients = new ArrayList<>();

    public ThriftClientPool(
        @Nonnull String name,
        @Nonnull HiveMetastoreThriftClient.Builder thriftClientBuilder,
        int threadPoolSize) {
      this.name = Preconditions.checkNotNull(name, "name was null.");
      this.executorService =
          ExecutorManager.newExecutorServiceWithBackpressure(name, threadPoolSize);
      this.executorManager = new ExecutorManager(executorService);
      this.threadLocalThriftClient =
          ThreadLocal.withInitial(
              () -> {
                String threadName = Thread.currentThread().getName();
                LOG.debug(
                    "Creating new thread-local Thrift client '{}' owned by pooled client '{}'.",
                    threadName,
                    name);
                try {
                  HiveMetastoreThriftClient client =
                      new HiveMetastoreThriftClient.Builder(thriftClientBuilder)
                          .withName(threadName)
                          .build();
                  synchronized (lock) {
                    builtClients.add(client);
                  }
                  return client;
                } catch (TTransportException e) {
                  throw new RuntimeException(
                      "Unable to build Thrift client '"
                          + threadName
                          + "' owned by pooled client '"
                          + name
                          + "' due to transport exception.",
                      e);
                }
              });
    }

    public void execute(ThriftClientConsumer consumer) {
      executorManager.execute(
          () -> {
            consumer.accept(getThreadLocalThriftClient().get());
            return null;
          });
    }

    @Nonnull
    private ThreadLocal<@NonNull ? extends HiveMetastoreThriftClient> getThreadLocalThriftClient() {
      return threadLocalThriftClient;
    }

    @Override
    public void close() throws Exception {
      LOG.debug("Shutting down thread pool backing pooled Thrift client '{}'", name);
      executorManager.close();
      MoreExecutors.shutdownAndAwaitTermination(executorService, 30, TimeUnit.SECONDS);
      synchronized (lock) {
        for (HiveMetastoreThriftClient client : builtClients) {
          try {
            LOG.debug(
                "Closing thread-local Thrift client '{}' owned by pooled client '{}'.",
                client.getName(),
                name);
            client.close();
          } catch (Exception ioe) {
            LOG.warn(
                "Unable to close Thrift client '"
                    + client.getName()
                    + "' owned by pooled client '"
                    + name
                    + "'. "
                    + client.getName(),
                ioe);
          }
        }
      }
      LOG.debug("Pooled Thrift client '{}' is now closed.", name);
    }
  }

  protected static class ThriftClientHandle extends AbstractHandle {

    @Nonnull private final HiveMetastoreThriftClient.Builder thriftClientBuilder;
    @Nonnegative private final int threadPoolSize;

    public ThriftClientHandle(
        @Nonnull HiveMetastoreThriftClient.Builder thriftClientBuilder,
        @Nonnegative int threadPoolSize) {
      this.thriftClientBuilder =
          Preconditions.checkNotNull(thriftClientBuilder, "Thrift client builder was null.");
      this.threadPoolSize = threadPoolSize;
    }

    /** Returns a thread-unsafe Thrift client unsuitable for use in multi-threaded contexts. */
    @Nonnull
    public HiveMetastoreThriftClient newClient(@Nonnull String name) throws TTransportException {
      LOG.debug("Creating a new Thrift client named '{}'.", name);
      return new HiveMetastoreThriftClient.Builder(thriftClientBuilder).withName(name).build();
    }

    /** Returns a thread-safe Thrift client pool suitable for use in multi-threaded contexts. */
    @Nonnull
    public ThriftClientPool newMultiThreadedThriftClientPool(@Nonnull String name) {
      LOG.debug(
          "Creating a new multi-threaded pooled Thrift client named '{}' backed by a thread pool of size {}.",
          name,
          threadPoolSize);
      return new ThriftClientPool(name, thriftClientBuilder, threadPoolSize);
    }
  }

  protected abstract static class AbstractHiveTask extends AbstractTask<Void> {

    public AbstractHiveTask(String targetPath) {
      super(targetPath);
    }

    @ForOverride
    protected abstract void run(@Nonnull Writer writer, @Nonnull ThriftClientHandle handle)
        throws Exception;

    @Override
    protected Void doRun(TaskRunContext context, ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      ThriftClientHandle thriftClientHandle = (ThriftClientHandle) handle;

      LOG.info("Writing to " + getTargetPath() + " -> " + sink);

      try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
        run(writer, thriftClientHandle);
      }

      return null;
    }

    @ForOverride
    protected abstract String toCallDescription();

    @Override
    public String toString() {
      return "Write " + getTargetPath() + " from " + toCallDescription();
    }
  }

  public AbstractHiveConnector(@Nonnull String name) {
    super(name);
  }

  @Nonnull
  @Override
  public Handle open(ConnectorArguments arguments) throws Exception {
    String requestedClientVersion = arguments.getHiveMetastoreVersion();
    HiveMetastoreThriftClient.Builder thriftClientBuilder =
        new HiveMetastoreThriftClient.Builder(requestedClientVersion)
            .withHost(arguments.getHost())
            .withPort(
                arguments.getPort(
                    Integer.parseInt(ConnectorArguments.OPT_HIVE_METASTORE_PORT_DEFAULT)))
            .withUnavailableClientVersionBehavior(
                HiveMetastoreThriftClient.Builder.UnavailableClientVersionBehavior.FALLBACK);
    return new ThriftClientHandle(thriftClientBuilder, arguments.getThreadPoolSize());
  }
}
