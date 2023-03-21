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
package com.google.edwmigration.dumper.plugin.ext.bigquery;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.cloud.RetryHelper;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.common.base.Throwables;
import com.swrve.ratelimitedlogger.RateLimitedLog;
import java.io.IOException;
import java.time.Duration;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author shevek */
public class BigQueryClientUtils {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryClientUtils.class);

  private static final Logger LOG_LIMITED =
      RateLimitedLog.withRateLimit(LOG).maxRate(2).every(Duration.ofSeconds(10)).build();

  @CheckForNull
  public static String getErrorReason(@Nonnull BigQueryException e) {
    BigQueryError error = e.getError();
    return error == null ? null : error.getReason();
  }

  public static boolean isRetryable(@Nonnull BigQueryException e) {
    if (e.isRetryable()) return true;
    // And now we handle the stuff for which BigQuery itself thinks that we can't retry.
    String reason = getErrorReason(e);
    if (reason == null) return false;
    switch (reason) {
      case "rateLimitExceeded":
        return true;
      default:
        return false;
    }
  }

  @CheckForNull
  private static BigQueryException getBigQueryException(
      @Nonnull RetryHelper.RetryHelperException e) {
    for (Throwable t : Throwables.getCausalChain(e))
      if (t instanceof BigQueryException) return (BigQueryException) t;
    return null;
  }

  @Nonnull
  public static <T> T runWithBackOff(@Nonnull BigQueryCallable<T> callable)
      throws BigQueryException, IOException, InterruptedException {
    BackOff backoff =
        new ExponentialBackOff(); // Default max elapsed time is 15 minutes, specified in
    // ExponentialBackOff itself.
    int retryCount = 0;
    for (; ; ) {
      BigQueryException exception;
      try {
        // For the inner BigQuery call, the max elapsed time is 50 seconds before we re-hit the
        // outer loop.
        // This is specified in ServiceOptions.DEFAULT_RETRY_SETTINGS.
        T result = callable.call();
        if (retryCount > 0)
          LOG_LIMITED.debug("BigQuery operation succeeded after {} retries.", retryCount);
        return result;
      } catch (RetryHelper.RetryHelperException e) {
        exception = getBigQueryException(e);
        if (exception == null) throw e;
      } catch (BigQueryException e) {
        exception = e;
      }
      // Now exception is non-null.
      if (!isRetryable(exception)) {
        // LOG.error("BigQuery retryable exception: non-retryable error " + e.getError(), e);
        throw exception;
      }
      long backOffMillis =
          backoff.nextBackOffMillis(); // throws IOException for some unknown reason.
      if (backOffMillis == BackOff.STOP) {
        LOG.error("Stopped retrying after {} retries: {}", retryCount, exception);
        throw exception;
      }
      LOG.warn("BigQuery retryable exception: sleeping for {}ms: {}", backOffMillis, exception);
      Thread.sleep(backOffMillis);
      retryCount++;
    }
  }
}
