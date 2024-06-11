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
package com.google.edwmigration.dumper.application.dumper.connector.hive;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.v2_3_6.Database;
import org.apache.thrift.TException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ThriftJsonSerializerTest {

  @Test
  public void serialize_success() throws TException {
    com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.v2_3_6.Database database =
        new Database(
            "sample-db",
            "test database",
            "http://sample-db-host/123",
            ImmutableMap.of("sampleParam1", "value456"));
    ThriftJsonSerializer serializer = new ThriftJsonSerializer();

    // Act
    String thriftObjectJson = serializer.serialize(database);

    // Assert
    assertEquals(
        "{\"name\":\"sample-db\","
            + "\"description\":\"test database\","
            + "\"locationUri\":\"http://sample-db-host/123\","
            + "\"parameters\":{\"sampleParam1\":\"value456\"}}",
        thriftObjectJson);
  }
}
