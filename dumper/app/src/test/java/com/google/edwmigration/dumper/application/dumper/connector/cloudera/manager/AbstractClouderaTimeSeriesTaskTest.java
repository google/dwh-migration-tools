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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.AbstractClouderaTimeSeriesTask.TimeSeriesAggregation;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.junit.Test;

public class AbstractClouderaTimeSeriesTaskTest {

  @Test
  public void doRun_missedAggregationParameter_throwsException() throws IOException {
    // WHEN: CPU usage task is initiated with no aggregation parameter
    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () ->
                new AbstractClouderaTimeSeriesTask("some path", 5, null, null) {
                  @Override
                  protected void doRun(
                      TaskRunContext context,
                      @Nonnull ByteSink sink,
                      @Nonnull ClouderaManagerHandle handle)
                      throws Exception {}
                });

    // THEN: There is a relevant exception has been raised
    assertEquals("TimeSeriesAggregation must not be null.", exception.getMessage());
  }

  @Test
  public void doRun_missedCategoryParameter_throwsException() throws IOException {
    // WHEN: CPU usage task is initiated with no aggregation parameter
    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () ->
                new AbstractClouderaTimeSeriesTask(
                    "some path", 5, TimeSeriesAggregation.DAILY, null) {
                  @Override
                  protected void doRun(
                      TaskRunContext context,
                      @Nonnull ByteSink sink,
                      @Nonnull ClouderaManagerHandle handle)
                      throws Exception {}
                });

    // THEN: There is a relevant exception has been raised
    assertEquals("TaskCategory must not be null.", exception.getMessage());
  }
}
