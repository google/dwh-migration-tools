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
package com.google.edwmigration.dumper.application.dumper.connector.hadoop;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MetastoreConnectionDetailsScriptTest {

  private static final long SCRIPT_TIMEOUT_SECONDS = 30;

  private static final String KEY = "| javax.jdo.option.ConnectionURL=";

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private Path script;

  private Path binDir;

  private Path beelineOutput;

  private Path beelineError;

  private Path scriptOutput;

  private Path scriptError;

  @Before
  public void setUp() throws IOException {
    script = temporaryFolder.getRoot().toPath().resolve("metastore-connection-details.sh");
    Files.write(script, HadoopScripts.read("metastore-connection-details.sh"));
    binDir = temporaryFolder.newFolder("bin").toPath();
    beelineOutput = temporaryFolder.getRoot().toPath().resolve("beeline-output.txt");
    beelineError = temporaryFolder.getRoot().toPath().resolve("beeline-error.txt");
    scriptOutput = temporaryFolder.getRoot().toPath().resolve("script-output.txt");
    scriptError = temporaryFolder.getRoot().toPath().resolve("script-error.txt");
    Files.write(beelineError, new byte[0]);
    Path beeline = binDir.resolve("beeline");
    Files.write(
        beeline,
        String.format(
                "#!/bin/bash\ncat '%s'\ncat '%s' >&2\nexit \"${FAKE_BEELINE_EXIT:-0}\"\n",
                beelineOutput, beelineError)
            .getBytes(UTF_8));
    assertTrue(beeline.toFile().setExecutable(true));
  }

  @Test
  public void run_urlWithoutCredentials_unchanged() throws Exception {
    assertRedacted(
        "jdbc:mysql://metastore-host:3306/metastore", "jdbc:mysql://metastore-host:3306/metastore");
  }

  @Test
  public void run_userAndPasswordInParameters_valuesRedacted() throws Exception {
    assertRedacted(
        "jdbc:mysql://metastore-host/metastore?createDatabaseIfNotExist=<redacted>"
            + "&user=<redacted>&password=<redacted>",
        "jdbc:mysql://metastore-host/metastore?createDatabaseIfNotExist=true"
            + "&user=hive&password=Secret1");
  }

  @Test
  public void run_userInfoBeforeHost_removed() throws Exception {
    assertRedacted(
        "jdbc:postgresql://metastore-host:5432/metastore",
        "jdbc:postgresql://hive:Se@cret1@metastore-host:5432/metastore");
  }

  @Test
  public void run_userInfoWithSemicolonCommaOrParens_removed() throws Exception {
    assertRedacted(
        "jdbc:postgresql://metastore-host:5432/metastore",
        "jdbc:postgresql://hive:p;ss@metastore-host:5432/metastore");
    assertRedacted(
        "jdbc:mysql://metastore-host/metastore", "jdbc:mysql://hive:p,ss@metastore-host/metastore");
    assertRedacted(
        "jdbc:mysql://metastore-host/metastore",
        "jdbc:mysql://hive:p(s)s@metastore-host/metastore");
  }

  @Test
  public void run_userInfoWithSeparatorBeforeHostList_hostsKept() throws Exception {
    assertRedacted(
        "jdbc:mysql://host1:3306,host2:3306/metastore",
        "jdbc:mysql://hive:p;ss@host1:3306,host2:3306/metastore");
  }

  @Test
  public void run_userInfoForEachHost_removedWithEarlierHosts() throws Exception {
    assertRedacted(
        "jdbc:mysql://host2:3306/metastore",
        "jdbc:mysql://hive:Secret1@host1:3306,hive:Secret1@host2:3306/metastore");
  }

  @Test
  public void run_hostListWithoutCredentials_unchanged() throws Exception {
    assertRedacted(
        "jdbc:mysql://host1:3306,host2:3306/metastore",
        "jdbc:mysql://host1:3306,host2:3306/metastore");
  }

  @Test
  public void run_atOnlyInParameterValues_hostKept() throws Exception {
    assertRedacted(
        "jdbc:mysql://metastore-host:3306/metastore?user=<redacted>&password=<redacted>",
        "jdbc:mysql://metastore-host:3306/metastore?user=hive@corp&password=Se@cret1");
  }

  @Test
  public void run_semicolonParametersContainingAt_hostKept() throws Exception {
    assertRedacted(
        "jdbc:sqlserver://metastore-host:1433;databaseName=<redacted>;user=<redacted>"
            + ";password=<redacted>",
        "jdbc:sqlserver://metastore-host:1433;databaseName=metastore;user=hive@corp"
            + ";password=Se@cret1");
  }

  @Test
  public void run_oracleUrlWithProtocol_hostKept() throws Exception {
    assertRedacted(
        "jdbc:oracle:thin:@tcps://metastore-host:2484/metastore",
        "jdbc:oracle:thin:hive/Se@cret1@tcps://metastore-host:2484/metastore");
  }

  @Test
  public void run_oracleUserAndPassword_removed() throws Exception {
    assertRedacted(
        "jdbc:oracle:thin:@metastore-host:1521:ORCL",
        "jdbc:oracle:thin:hive/Secret1@metastore-host:1521:ORCL");
  }

  @Test
  public void run_oraclePasswordWithAt_removed() throws Exception {
    assertRedacted(
        "jdbc:oracle:thin:@metastore-host:1521:ORCL",
        "jdbc:oracle:thin:hive/Se@cret1@metastore-host:1521:ORCL");
  }

  @Test
  public void run_urlInStandardError_redacted() throws Exception {
    Files.write(beelineOutput, new byte[0]);
    String errorFormat = "Unable to open a test connection. JDBC url = %s, username = hive.\n";
    Files.write(
        beelineError,
        String.format(
                errorFormat, "jdbc:mysql://hive:Secret1@metastore-host/metastore?password=Secret1")
            .getBytes(UTF_8));

    assertEquals(0, runScript(0));

    // A parameter value ends at the next space, so the comma after the URL is redacted with it.
    assertEquals(
        "Unable to open a test connection. JDBC url = jdbc:mysql://metastore-host/metastore"
            + "?password=<redacted> username = hive.\n",
        new String(Files.readAllBytes(scriptError), UTF_8));
  }

  @Test
  public void run_oracleServiceUrl_unchanged() throws Exception {
    assertRedacted(
        "jdbc:oracle:thin:@//metastore-host:1521/metastore",
        "jdbc:oracle:thin:@//metastore-host:1521/metastore");
  }

  @Test
  public void run_semicolonParameters_valuesRedacted() throws Exception {
    assertRedacted(
        "jdbc:sqlserver://metastore-host:1433;databaseName=<redacted>;user=<redacted>"
            + ";password=<redacted>",
        "jdbc:sqlserver://metastore-host:1433;databaseName=metastore;user=hive;password=Secret1");
  }

  @Test
  public void run_parenthesizedProperties_valuesRedacted() throws Exception {
    assertRedacted(
        "jdbc:mysql://address=(host=<redacted>)(user=<redacted>)(password=<redacted>)/metastore",
        "jdbc:mysql://address=(host=metastore-host)(user=hive)(password=Secret1)/metastore");
  }

  @Test
  public void run_parenthesizedPropertiesWithAtInCredentials_valuesRedacted() throws Exception {
    assertRedacted(
        "jdbc:mysql://address=(host=<redacted>)(user=<redacted>)(password=<redacted>)/metastore",
        "jdbc:mysql://address=(host=metastore-host)(user=hive@corp)(password=Se@cret1)/metastore");
  }

  @Test
  public void run_oracleUrlWithQueryContainingAt_queryRedactedAndHostKept() throws Exception {
    assertRedacted(
        "jdbc:oracle:thin:@metastore-host:1521:ORCL?user=<redacted>&password=<redacted>",
        "jdbc:oracle:thin:hive/Se@cret1@metastore-host:1521:ORCL?user=hive@corp&password=Se@cret1");
  }

  @Test
  public void run_oracleUrlWithSemicolonContainingAt_paramsRedactedAndHostKept() throws Exception {
    assertRedacted(
        "jdbc:oracle:thin:@metastore-host:1521:ORCL;user=<redacted>;password=<redacted>",
        "jdbc:oracle:thin:@metastore-host:1521:ORCL;user=hive@corp;password=Se@cret1");
  }

  @Test
  public void run_oracleTnsDescriptorContainingAt_descriptorRedacted() throws Exception {
    assertRedacted(
        "jdbc:oracle:thin:@(DESCRIPTION=<redacted>(ADDRESS=<redacted>(HOST=<redacted>)(PORT=<redacted>))"
            + "(CONNECT_DATA=<redacted>(SERVICE_NAME=<redacted>)))",
        "jdbc:oracle:thin:hive/Se@cret1@(DESCRIPTION=(ADDRESS=(HOST=metastore-host)(PORT=1521))"
            + "(CONNECT_DATA=(SERVICE_NAME=svc@corp)))");
  }

  @Test
  public void run_oracleUrlWithParenthesizedAt_notTruncated() throws Exception {
    assertRedacted("jdbc:oracle:thin:@(host@corp)", "jdbc:oracle:thin:hive/Se@cret1@(host@corp)");
  }

  @Test
  public void run_oraclePasswordWithQuestionSemicolonAndParens_removed() throws Exception {
    assertRedacted(
        "jdbc:oracle:thin:@metastore-host:1521:ORCL",
        "jdbc:oracle:thin:hive/p?s;s(a@b)@metastore-host:1521:ORCL");
  }

  @Test
  public void run_parameterValueWithSemicolonCommaOrParens_redacted() throws Exception {
    assertRedacted(
        "jdbc:mysql://metastore-host/metastore?user=<redacted>&password=<redacted>"
            + "&useSSL=<redacted>",
        "jdbc:mysql://metastore-host/metastore?user=hive&password=Se;cr,e(t)1&useSSL=true");
  }

  @Test
  public void run_semicolonParameterValueWithOtherSeparators_redacted() throws Exception {
    assertRedacted(
        "jdbc:sqlserver://metastore-host:1433;databaseName=<redacted>;password=<redacted>"
            + ";encrypt=<redacted>",
        "jdbc:sqlserver://metastore-host:1433;databaseName=metastore;password=Se,cr(e)t&1?x"
            + ";encrypt=true");
  }

  @Test
  public void run_semicolonParameterValueInBraces_redacted() throws Exception {
    assertRedacted(
        "jdbc:sqlserver://metastore-host:1433;databaseName=<redacted>;password=<redacted>"
            + ";encrypt=<redacted>",
        "jdbc:sqlserver://metastore-host:1433;databaseName=metastore;password={Se;cr}};et1}"
            + ";encrypt=true");
  }

  @Test
  public void run_semicolonParameterValueWithSeparatorAndAt_hostKept() throws Exception {
    assertRedacted(
        "jdbc:sqlserver://metastore-host:1433;databaseName=<redacted>;user=<redacted>"
            + ";password=<redacted>",
        "jdbc:sqlserver://metastore-host:1433;databaseName=metastore;user=hive@corp"
            + ";password=Se,c@ret1");
    assertRedacted(
        "jdbc:sqlserver://metastore-host:1433;databaseName=<redacted>;user=<redacted>"
            + ";password=<redacted>",
        "jdbc:sqlserver://metastore-host:1433;databaseName=metastore;user=hive@corp"
            + ";password={Se;c@ret1}");
  }

  @Test
  public void run_parenthesizedPropertyValueWithSemicolonOrAmpersand_redacted() throws Exception {
    assertRedacted(
        "jdbc:mysql://address=(host=<redacted>)(user=<redacted>)(password=<redacted>)/metastore",
        "jdbc:mysql://address=(host=metastore-host)(user=hive)(password=Se;cr&et1)/metastore");
    assertRedacted(
        "jdbc:mysql://(host=<redacted>,user=<redacted>,password=<redacted>)/metastore",
        "jdbc:mysql://(host=metastore-host,user=hive,password=Se;cr&et1)/metastore");
  }

  @Test
  public void run_userInfoContainingParameterSyntax_removed() throws Exception {
    assertRedacted(
        "jdbc:mysql://metastore-host:3306/metastore",
        "jdbc:mysql://hive:p;a=b@metastore-host:3306/metastore");
    assertRedacted(
        "jdbc:mariadb://metastore-host:3306/metastore",
        "jdbc:mariadb://hive:p,a=b@metastore-host:3306/metastore");
    assertRedacted(
        "jdbc:postgresql://metastore-host:5432/metastore",
        "jdbc:postgresql://hive:p;a=b@metastore-host:5432/metastore");
    assertRedacted(
        "jdbc:mysql:loadbalance://host1:3306,host2:3306/metastore",
        "jdbc:mysql:loadbalance://hive:p;a=b@host1:3306,host2:3306/metastore");
  }

  @Test
  public void run_parametersWithoutSeparatorsInValues_redactedAsBefore() throws Exception {
    assertRedacted(
        "jdbc:mysql://host1:3306,host2:3306/metastore?createDatabaseIfNotExist=<redacted>"
            + "&useSSL=<redacted>",
        "jdbc:mysql://host1:3306,host2:3306/metastore?createDatabaseIfNotExist=true&useSSL=false");
    assertRedacted(
        "jdbc:derby:;databaseName=<redacted>;create=<redacted>",
        "jdbc:derby:;databaseName=metastore_db;create=true");
    assertRedacted(
        "jdbc:oracle:thin:@(DESCRIPTION=<redacted>(ADDRESS=<redacted>(PROTOCOL=<redacted>)"
            + "(HOST=<redacted>)(PORT=<redacted>))(CONNECT_DATA=<redacted>"
            + "(SERVICE_NAME=<redacted>)))",
        "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=metastore-host)(PORT=1521))"
            + "(CONNECT_DATA=(SERVICE_NAME=metastore)))");
  }

  @Test
  public void run_otherLines_unchanged() throws Exception {
    String output = "+------+\n|  set  |\n+------+\nhive.execution.engine=tez\n+------+\n";
    Files.write(beelineOutput, output.getBytes(UTF_8));

    assertEquals(0, runScript(0));

    assertEquals(output, readScriptOutput());
  }

  @Test
  public void run_beelineFails_exitStatusKept() throws Exception {
    Files.write(beelineOutput, new byte[0]);

    assertEquals(3, runScript(3));
  }

  private void assertRedacted(String expectedUrl, String url) throws Exception {
    Files.write(beelineOutput, (KEY + url + " |\n").getBytes(UTF_8));

    assertEquals(0, runScript(0));

    assertEquals(KEY + expectedUrl + " |\n", readScriptOutput());
  }

  private int runScript(int beelineExitStatus) throws Exception {
    ProcessBuilder builder =
        new ProcessBuilder("/bin/bash", script.toString())
            .redirectOutput(scriptOutput.toFile())
            .redirectError(scriptError.toFile());
    Map<String, String> environment = builder.environment();
    environment.put("PATH", binDir + File.pathSeparator + environment.get("PATH"));
    environment.put("FAKE_BEELINE_EXIT", String.valueOf(beelineExitStatus));
    Process process = builder.start();
    assertTrue("Script did not finish.", process.waitFor(SCRIPT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    return process.exitValue();
  }

  private String readScriptOutput() throws IOException {
    return new String(Files.readAllBytes(scriptOutput), UTF_8);
  }
}
