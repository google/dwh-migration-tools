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

import static org.junit.Assert.*;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.edwmigration.dumper.common.UUIDGenerator;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.BigQueryLogsDumpFormat;
import com.google.edwmigration.dumper.test.TestUtils;
import java.io.File;
import java.text.SimpleDateFormat;
import org.joda.time.DateTime;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the BigQuery query logs dumper against the CompilerWorks BigQuery account and asserts on the
 * output.
 *
 * @author shevek
 */
@RunWith(JUnit4.class)
public class BigQueryLogsConnectorTest extends AbstractBigQueryConnectorExecutionTest {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryLogsConnectorTest.class);

  private static final String CANARY_TEMPLATE = "SELECT '%s'";

  private final BigQueryLogsConnector connector = new BigQueryLogsConnector();

  @Test
  public void testConnector() throws Exception {
    testConnectorDefaults(connector);
  }

  private String submitCanaryQuery() throws Exception {
    BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
    final String canaryQuery =
        String.format(CANARY_TEMPLATE, UUIDGenerator.getInstance().nextUUID());
    LOG.debug("Submitting canary query: {}", canaryQuery);
    TableResult tableResult = bigQuery.query(QueryJobConfiguration.of(canaryQuery));
    assertEquals(1, tableResult.getTotalRows());
    return canaryQuery;
  }

  @Ignore("Expensive. Move to integration tests?")
  @Test
  public void testExecution() throws Exception {

    final String canaryQuery = submitCanaryQuery();

    File outputFile = TestUtils.newOutputFile("test-compilerworks-bigquery-logs.zip");
    LOG.debug("Output file: {}", outputFile.getAbsolutePath());
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    String earliestDate =
        dateFormat.format(
            new DateTime().minusDays(1).toDate()); // we could run at midnight, so we want -1 days
    String latestDate = dateFormat.format(new DateTime().toDate());
    LOG.debug("Earliest date: {}; latest date: {}", earliestDate, latestDate);

    runDumper(
        "--connector",
        "bigquery-logs",
        "--output",
        outputFile.getAbsolutePath(),
        "--query-log-start",
        earliestDate,
        "--query-log-end",
        latestDate);

    ZipValidator validator =
        new ZipValidator()
            .withFormat(BigQueryLogsDumpFormat.FORMAT_NAME)
            .withExpectedEntries(BigQueryLogsDumpFormat.QueryHistoryJson.ZIP_ENTRY_NAME)
            .withAllowedEntries("query_history.csv");
    validator.run(outputFile);
  }
}
