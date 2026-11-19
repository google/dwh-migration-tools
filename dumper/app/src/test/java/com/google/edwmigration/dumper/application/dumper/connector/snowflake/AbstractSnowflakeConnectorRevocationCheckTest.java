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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.AbstractSnowflakeConnector.OCSP_FAIL_OPEN_PROPERTY;
import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.AbstractSnowflakeConnector.OCSP_FAIL_OPEN_SYSTEM_PROPERTY;
import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.AbstractSnowflakeConnector.dataSourceProperties;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.google.common.collect.ObjectArrays;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import java.io.IOException;
import java.util.Properties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Certificate revocation checking of the Snowflake driver, which is off unless it is asked for. */
@RunWith(JUnit4.class)
public class AbstractSnowflakeConnectorRevocationCheckTest {

  private static final String URL = "jdbc:snowflake://account.snowflakecomputing.com/";

  @Test
  public void dataSourceProperties_password_setsFailOpen() throws Exception {
    Properties properties = dataSourceProperties(arguments("--password", "secret"));

    assertEquals("true", properties.get(OCSP_FAIL_OPEN_PROPERTY));
  }

  @Test
  public void dataSourceProperties_privateKey_setsFailOpen() throws Exception {
    Properties properties = dataSourceProperties(arguments("--private-key-file", "key.p8"));

    assertEquals("true", properties.get(OCSP_FAIL_OPEN_PROPERTY));
  }

  @Test
  public void dataSourceProperties_urlWithoutRevocationSetting_setsFailOpen() throws Exception {
    Properties properties =
        dataSourceProperties(arguments("--password", "secret", "--url", URL + "?warehouse=WH"));

    assertEquals("true", properties.get(OCSP_FAIL_OPEN_PROPERTY));
  }

  @Test
  public void dataSourceProperties_urlKeepsChecksEnabled_setsFailOpen() throws Exception {
    // Before driver 4.4.0 this value was the default and the checks ran; it no longer enables them.
    Properties properties =
        dataSourceProperties(
            arguments("--password", "secret", "--url", URL + "?disableOCSPChecks=false"));

    assertEquals("true", properties.get(OCSP_FAIL_OPEN_PROPERTY));
  }

  @Test
  public void dataSourceProperties_urlSetsFailClosed_leavesUrlInCharge() throws Exception {
    Properties properties =
        dataSourceProperties(
            arguments("--password", "secret", "--url", URL + "?warehouse=WH&OCSPFAILOPEN=false"));

    assertFalse(properties.containsKey(OCSP_FAIL_OPEN_PROPERTY));
  }

  @Test
  public void dataSourceProperties_urlSetsFailClosedPercentEncoded_leavesUrlInCharge()
      throws Exception {
    // The driver decodes parameter names, so this is ocspFailOpen=false.
    Properties properties =
        dataSourceProperties(
            arguments("--password", "secret", "--url", URL + "?ocsp%46ailOpen=false"));

    assertFalse(properties.containsKey(OCSP_FAIL_OPEN_PROPERTY));
  }

  @Test
  public void dataSourceProperties_urlWithMalformedEscape_setsFailOpen() throws Exception {
    Properties properties =
        dataSourceProperties(arguments("--password", "secret", "--url", URL + "?role=A%ZZ"));

    assertEquals("true", properties.get(OCSP_FAIL_OPEN_PROPERTY));
  }

  @Test
  public void dataSourceProperties_urlDisablesChecks_leavesUrlInCharge() throws Exception {
    Properties properties =
        dataSourceProperties(
            arguments("--private-key-file", "key.p8", "--url", URL + "?disableocspchecks=TRUE"));

    assertFalse(properties.containsKey(OCSP_FAIL_OPEN_PROPERTY));
  }

  @Test
  public void dataSourceProperties_urlWithInsecureMode_leavesUrlInCharge() throws Exception {
    Properties properties =
        dataSourceProperties(
            arguments("--password", "secret", "--url", URL + "?insecureMode=on#fragment"));

    assertFalse(properties.containsKey(OCSP_FAIL_OPEN_PROPERTY));
  }

  @Test
  public void dataSourceProperties_systemPropertySet_leavesSystemPropertyInCharge()
      throws Exception {
    String previous = System.getProperty(OCSP_FAIL_OPEN_SYSTEM_PROPERTY);
    System.setProperty(OCSP_FAIL_OPEN_SYSTEM_PROPERTY, "false");
    try {
      Properties properties = dataSourceProperties(arguments("--password", "secret"));

      assertFalse(properties.containsKey(OCSP_FAIL_OPEN_PROPERTY));
    } finally {
      if (previous == null) {
        System.clearProperty(OCSP_FAIL_OPEN_SYSTEM_PROPERTY);
      } else {
        System.setProperty(OCSP_FAIL_OPEN_SYSTEM_PROPERTY, previous);
      }
    }
  }

  private static ConnectorArguments arguments(String... args) throws IOException {
    return new ConnectorArguments(
        ObjectArrays.concat(
            new String[] {"--connector", "snowflake", "--user", "testuser"}, args, String.class));
  }
}
