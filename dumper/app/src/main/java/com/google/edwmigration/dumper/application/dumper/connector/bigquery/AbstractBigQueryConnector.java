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
package com.google.edwmigration.dumper.application.dumper.connector.bigquery;

import com.google.api.client.http.HttpStatusCodes;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.common.base.Preconditions;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.io.ByteSink;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnector;
import com.google.edwmigration.dumper.application.dumper.handle.AbstractHandle;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.bigquery.BigQueryCallable;
import com.google.edwmigration.dumper.plugin.ext.bigquery.BigQueryClientUtils;
import com.google.edwmigration.dumper.plugin.ext.jdk.concurrent.ExecutorManager;
import com.google.errorprone.annotations.ForOverride;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RespectsInput(
    order = 0,
    env = ServiceOptions.CREDENTIAL_ENV_NAME,
    description = "The path to the Google credentials JSON file.",
    required = "yes")
public abstract class AbstractBigQueryConnector extends AbstractConnector {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractBigQueryConnector.class);

  public static class BigQueryHandle extends AbstractHandle {

    private final BigQuery bigQuery;

    public BigQueryHandle(@Nonnull BigQuery bigQuery) {
      this.bigQuery = Preconditions.checkNotNull(bigQuery, "BigQuery was null.");
    }

    @Nonnull
    public BigQuery getBigQuery() {
      return bigQuery;
    }
  }

  public abstract static class AbstractBigQueryTask extends AbstractTask<Void> {

    @Nonnull
    protected ExecutorService newExecutorService() {
      return ExecutorManager.newExecutorServiceWithBackpressure(getName(), 32);
    }

    protected void shutdown(@Nonnull ExecutorService executor) {
      MoreExecutors.shutdownAndAwaitTermination(executor, 30, TimeUnit.SECONDS);
    }

    // This can return null. Really, we want someone to use BigQueryCallable<@NonNull Foo> to
    // indicate whether their callable can return null.
    protected static <T> T runWithBackOff(@Nonnull BigQueryCallable<T> callable)
        throws BigQueryException, IOException, InterruptedException {
      return BigQueryClientUtils.runWithBackOff(callable);
    }

    /** Iterates over a page, with retries where appropriate. */
    protected static class PageIterable<T> implements Iterable<T> {

      // We don't extend AbstractIterator here because if we call next()
      // within hasNext() and it throws an exception, we can get very twisted up.
      private static class Itr<T> extends UnmodifiableIterator<T> {

        @CheckForNull private Page<T> currentPage;
        @Nonnull private Iterator<T> currentPageIterator;

        Itr(@Nonnull Page<T> currentPage) {
          this.currentPage = currentPage;
          this.currentPageIterator = currentPage.getValues().iterator();
        }

        @Override
        public boolean hasNext() {
          for (; ; ) {
            while (!currentPageIterator.hasNext()) {
              try {
                currentPage = runWithBackOff(() -> currentPage.getNextPage());
              } catch (IOException e) {
                throw new BigQueryException(e);
              } catch (InterruptedException e) {
                throw new BigQueryException(
                    HttpStatusCodes.STATUS_CODE_CONFLICT, "Interrupted while retrying: " + e, e);
              }
              if (currentPage == null) {
                return false;
              }
              currentPageIterator = currentPage.getValues().iterator();
            }
            // It is possible for hasNext() and next() to throw.
            // See https://github.com/googleapis/google-cloud-java/issues/6499
            // However, if we catch it, we can't guarantee that next() won't throw on every call
            // thereafter.
            return true;
          }
        }

        @Override
        public T next() {
          return currentPageIterator.next();
        }
      }

      private final Page<T> page;

      public PageIterable(@Nonnull Page<T> page) {
        this.page = Preconditions.checkNotNull(page, "Initial page was null.");
      }

      @Override
      public Iterator<T> iterator() {
        return new Itr<>(page);
      }
    }

    public AbstractBigQueryTask(@Nonnull String targetPath) {
      super(targetPath);
    }

    @ForOverride
    protected abstract void run(@Nonnull Writer writer, @Nonnull BigQuery bigQuery)
        throws Exception;

    @Override
    protected Void doRun(TaskRunContext context, ByteSink sink, Handle handle) throws Exception {
      BigQueryHandle bqHandle = (BigQueryHandle) handle;
      BigQuery bigQuery = bqHandle.getBigQuery();

      LOG.info("Writing to " + getTargetPath() + " -> " + sink);

      try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
        run(writer, bigQuery);
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

  public AbstractBigQueryConnector(@Nonnull String name) {
    super(name);
  }

  @Override
  @Nonnull
  public Handle open(@Nonnull ConnectorArguments arguments) throws Exception {
    // There is currently a bug in the BigQuery client libraries. The documentation says that if we
    // have a
    // file specified in the environment, it will take precedence over the credentials present in
    // the
    // account running the process. This doesn't actually work properly, so we have our own logic
    // here.
    String credentialsFile = System.getenv(ServiceOptions.CREDENTIAL_ENV_NAME);
    BigQuery bigQuery;
    if (credentialsFile != null) {
      bigQuery =
          BigQueryOptions.newBuilder()
              .setCredentials(
                  GoogleCredentials.fromStream(
                      FileUtils.openInputStream(FileUtils.getFile(credentialsFile))))
              .build()
              .getService();
    } else {
      bigQuery = BigQueryOptions.getDefaultInstance().getService();
    }
    return new BigQueryHandle(bigQuery);
  }
}
