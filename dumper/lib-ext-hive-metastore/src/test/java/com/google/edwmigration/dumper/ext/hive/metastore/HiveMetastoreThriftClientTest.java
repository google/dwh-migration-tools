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
package com.google.edwmigration.dumper.ext.hive.metastore;

import static org.junit.Assert.assertTrue;

import java.util.List;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class HiveMetastoreThriftClientTest {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(HiveMetastoreThriftClientTest.class);

  @Ignore("For local execution only; Jenkins doesn't have a live Hive metastore running.")
  @Test
  public void testClientAgainstLiveMetastore_v2_3_6() throws Exception {
    HiveMetastoreThriftClient client =
        new HiveMetastoreThriftClient.Builder("2.3.6").withHost("localhost").withPort(9083).build();
    List<? extends String> databaseNames = client.getAllDatabaseNames();
    LOG.info("Databases in metastore: {}", databaseNames);
    assertTrue("Expected at least one database name.", databaseNames.size() > 0);
  }

  /**
   * Ensures that we fallback to the superset Thrift specification when we request a client for a
   * version which is unavailable or does not exist.
   */
  @Ignore("For local execution only; Jenkins doesn't have a live Hive metastore running.")
  @Test
  public void testClientAgainstLiveMetastore_Fallback() throws Exception {
    HiveMetastoreThriftClient client =
        new HiveMetastoreThriftClient.Builder("100.200.300")
            .withHost("localhost")
            .withPort(9083)
            .build();
    List<? extends String> databaseNames = client.getAllDatabaseNames();
    LOG.info("Databases in metastore: {}", databaseNames);
    assertTrue("Expected at least one database name.", databaseNames.size() > 0);
  }
}
