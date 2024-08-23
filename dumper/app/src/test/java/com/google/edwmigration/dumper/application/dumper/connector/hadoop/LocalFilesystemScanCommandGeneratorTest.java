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
package com.google.edwmigration.dumper.application.dumper.connector.hadoop;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class LocalFilesystemScanCommandGeneratorTest {

  @Test
  public void generate_success() {
    String command = LocalFilesystemScanCommandGenerator.generate();
    assertEquals(
        "find / -iname 'phoenix*.jar' -o -iname '*coprocessor*.jar' -o -iname '*jdbc*.jar' -o -iname '*odbc*.jar' -o -iname 'salesforce' -o -iname 'ngdbc.jar' -o -iname '*connector*.jar' -o -iname 'oozie-site.xml' -o -iname 'splunk' -o -iname 'newrelic-infra.yml' -o -iname 'elasticsearch.yml' -o -iname 'ganglia.conf' 2>/dev/null",
        command);
  }
}
