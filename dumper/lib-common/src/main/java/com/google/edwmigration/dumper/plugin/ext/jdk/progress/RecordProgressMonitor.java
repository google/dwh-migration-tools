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
package com.google.edwmigration.dumper.plugin.ext.jdk.progress;

import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import net.jcip.annotations.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author shevek */
@NotThreadSafe
public class RecordProgressMonitor extends AbstractProgressMonitor {

  private static final Logger LOG = LoggerFactory.getLogger(RecordProgressMonitor.class);

  private final String name;
  private final long total;
  private final long stepPercent;
  private static final long stepSeconds = 10;
  // limit progress output to no more than every "min-seconds", this is to avoid flooding the log
  private static final long stepMinSeconds = 3;
  private long count;
  private final Stopwatch stopwatch = Stopwatch.createStarted();
  // Progress
  private long nextCount = 0;
  private long nextSeconds = 0;
  private long nextMinSeconds = 0;

  public RecordProgressMonitor(@Nonnull String name, @Nonnegative long total) {
    this.name = name;
    this.total = total;
    this.stepPercent = Math.max(total / 10, 1); // 10% or 1 count.

    this.nextCount = stepPercent;
    this.nextSeconds = stepSeconds;
    this.nextMinSeconds = stepMinSeconds;

    LOG.debug(name + ": Starting with " + total + " items. " + newMemorySummary());
  }

  public RecordProgressMonitor(@Nonnull String name) {
    this.name = name;
    this.total = 0;
    this.stepPercent = Integer.MAX_VALUE;

    this.nextCount = Integer.MAX_VALUE;
    this.nextSeconds = stepSeconds;
    this.nextMinSeconds = stepMinSeconds;

    LOG.debug(name + ": Starting. " + newMemorySummary());
  }

  @Override
  public long timeElapsed(TimeUnit desiredUnit) {
    return stopwatch.elapsed(desiredUnit);
  }

  @Nonnegative
  public long getTotal() {
    return total;
  }

  @Override
  public long getCount() {
    return count;
  }

  private void update(@Nonnull String status) {
    final long currSeconds = stopwatch.elapsed(TimeUnit.SECONDS);
    nextSeconds = currSeconds + stepSeconds;
    nextMinSeconds = currSeconds + stepMinSeconds;
    // A bit fiddly, but permits both the comparison with 0 and the double divide.
    long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    String rate =
        (elapsed == 0) ? "infinity" : Long.toString((long) (count * 1000L / (double) elapsed));
    if (total > 0) {
      nextCount = count + stepPercent;
      LOG.debug(
          name
              + ": "
              + status
              + " "
              + count
              + " / "
              + total
              + " items ("
              + String.format("%2.2f", 100d * count / total)
              + "%) in "
              + stopwatch
              + " ("
              + rate
              + "/sec) "
              + newMemorySummary());
    } else {
      LOG.debug(
          name
              + ": "
              + status
              + " "
              + count
              + " items in "
              + stopwatch
              + " ("
              + rate
              + "/sec) "
              + newMemorySummary());
    }
  }

  @Override
  public long count(int delta) {
    long ret = (count += delta);
    long currSeconds = stopwatch.elapsed(TimeUnit.SECONDS);
    if (count >= nextCount && currSeconds >= nextMinSeconds) {
      update("Processed");
      return ret;
    }
    if (currSeconds >= nextSeconds) {
      update("Processed");
      return ret;
    }
    return ret;
  }

  @Override
  public void close() {
    update("Completed");
    // LOG.debug(name + ": Done in " + stopwatch + ".");
  }
}
