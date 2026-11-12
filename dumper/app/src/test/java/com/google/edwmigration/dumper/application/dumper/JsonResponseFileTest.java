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
package com.google.edwmigration.dumper.application.dumper;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ObjectArrays;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** */
@RunWith(JUnit4.class)
public class JsonResponseFileTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  // replace single quotes to double quotes to avoid \" messup.
  private void check(String json, String... exp) throws IOException, JsonProcessingException {
    Assert.assertArrayEquals(exp, JsonResponseFile.to_arguments(json.replace("'", "\"")).toArray());
  }

  @Test
  public void testJson2Args() throws IOException, JsonProcessingException {
    check("{'help':''}", "--help");
    check("{'a':{'b':['c','d']}}", "--a-b", "c:d");
    check("a:\n  b:\n    - c\n    - d\n", "--a-b", "c:d");
    check(
        "{"
            + "    'connector' : 'redshift-logs',"
            + "    'driver' : ["
            + "        'some-path-to-redhsift.jdbc.jar',"
            + "        'any-other-jar-to-load.jar' ],"
            + "    'redshift' : {"
            + "        'iam' : {"
            + "            'accessid' : 'something',"
            + "            'sercret' : 'secret'"
            + "        }"
            + "    },"
            + "    's3' : {"
            + "        'iam' : {"
            + "            'accessid' : 'something',"
            + "            'sercret' : 'secret'"
            + "        },"
            + "        'bucket' : 'bucket id'"
            + "    }    "
            + "}",
        "--connector",
        "redshift-logs",
        "--driver",
        "some-path-to-redhsift.jdbc.jar:any-other-jar-to-load.jar",
        "--redshift-iam-accessid",
        "something",
        "--redshift-iam-sercret",
        "secret",
        "--s3-iam-accessid",
        "something",
        "--s3-iam-sercret",
        "secret",
        "--s3-bucket",
        "bucket id");

    check("{'definitions': {'a.b': 'test' } }", "-Da.b=test");
  }

  @Test
  public void testArgs2Json() throws IOException, JsonProcessingException {
    String[] args = {
      "--connector",
      "redshift-logs",
      "--driver",
      "some-path-to-redhsift.jdbc.jar:any-other-jar-to-load.jar",
      "--redshift-iam-accessid",
      "something",
      "--redshift-iam-sercret",
      "secret",
      "--s3-iam-accessid",
      "something",
      "--s3-iam-sercret",
      "secret",
      "--s3-bucket",
      "bucket id",
      "-Dredshift.some.prop=some value",
      "-Dredshift.some.other.prop=some other value = 123",
    };

    // Round trip
    Assert.assertArrayEquals(
        args,
        JsonResponseFile.to_arguments(
                new ObjectMapper().writeValueAsString(JsonResponseFile.from_arguments(args)))
            .toArray(new String[] {}));
  }

  @Test
  public void addResponseFiles_emptyArray() throws IOException {
    // Act
    String[] result = JsonResponseFile.addResponseFiles(new String[0]);

    // Assert
    assertEquals(0, result.length);
  }

  @Test
  public void addResponseFiles_emptyElement() throws IOException {
    String[] args = new String[] {""};

    // Act
    String[] result = JsonResponseFile.addResponseFiles(args);

    // Assert
    assertArrayEquals(args, result);
  }

  @Test
  public void save_omitsSecretValues() throws IOException {
    String saved =
        saveResponseFile(
            "--connector",
            "redshift",
            "--host",
            "db.example.com",
            "--password",
            "value-1",
            "--private-key-password",
            "value-2",
            "--iam-secretaccesskey",
            "value-3",
            "--iam-sessiontoken",
            "value-4");

    assertNoneContained(saved, "value-1", "value-2", "value-3", "value-4");
    assertEquals(
        ImmutableList.of("--connector", "redshift", "--host", "db.example.com", "--password"),
        JsonResponseFile.to_arguments(saved));
  }

  @Test
  public void save_omitsSecretValuesOfAbbreviatedAndInlineOptions() throws IOException {
    String saved =
        saveResponseFile(
            "--connector",
            "redshift",
            "--pass=value-1",
            "--private-key-pass",
            "value-2",
            "-iam-secretaccesskey",
            "value-3",
            "--iam-sessiontoken=value-4");

    assertNoneContained(saved, "value-1", "value-2", "value-3", "value-4");
    assertEquals(
        ImmutableList.of("--connector", "redshift", "--password"),
        JsonResponseFile.to_arguments(saved));
  }

  @Test
  public void save_omitsSecretValuesThatStartWithHyphen() throws IOException {
    String saved =
        saveResponseFile(
            "--connector",
            "redshift",
            "--private-key-password",
            "-value-1",
            "--password",
            "--iam-sessiontoken",
            "--value-2");

    assertNoneContained(saved, "value-1", "value-2");
    assertEquals(
        ImmutableList.of("--connector", "redshift", "--password"),
        JsonResponseFile.to_arguments(saved));
  }

  @Test
  public void save_omitsUrlWithCredentials() throws IOException {
    String[] urls = {
      "jdbc:postgresql://admin:value-1@db.example.com:5432/db",
      "jdbc:oracle:thin:admin/value-1@//db.example.com:1521/service",
      "jdbc:snowflake://account.example.com/?user=admin&password=value-1",
      "jdbc:databricks://db.example.com:443;AuthMech=3;UID=token;PWD=value-1",
    };

    for (String url : urls) {
      String saved = saveResponseFile("--connector", "test", "--url", url);

      assertNoneContained(saved, "value-1");
      assertEquals(ImmutableList.of("--connector", "test"), JsonResponseFile.to_arguments(saved));
    }
  }

  @Test
  public void save_keepsUrlWithoutCredentials() throws IOException {
    String[] urls = {
      "jdbc:oracle:thin:@//db.example.com:1521/service",
      "jdbc:postgresql://db.example.com:5432/db?ssl=true",
      "https://admin@db.example.com:7180/api/v1/",
    };

    for (String url : urls) {
      String saved = saveResponseFile("--connector", "test", "--url", url);

      assertEquals(
          ImmutableList.of("--connector", "test", "--url", url),
          JsonResponseFile.to_arguments(saved));
    }
  }

  @Test
  public void save_createsFileReadableByOwnerOnly() throws IOException {
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
    File file = new File(temporaryFolder.getRoot(), "response.json");

    JsonResponseFile.save(
        new ConnectorArguments("--connector", "test", "--save-response-file", file.getPath()));

    assertEquals(
        PosixFilePermissions.fromString("rw-------"), Files.getPosixFilePermissions(file.toPath()));
  }

  @Test
  public void save_restrictsExistingFileToOwner() throws IOException {
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
    File file = temporaryFolder.newFile("response.json");
    Files.setPosixFilePermissions(file.toPath(), PosixFilePermissions.fromString("rw-rw-r--"));

    JsonResponseFile.save(
        new ConnectorArguments("--connector", "test", "--save-response-file", file.getPath()));

    assertEquals(
        PosixFilePermissions.fromString("rw-------"), Files.getPosixFilePermissions(file.toPath()));
  }

  @Test
  public void findSecretOptionNames_returnsConnectorArgumentsSecrets() throws IOException {
    ConnectorArguments arguments = new ConnectorArguments("--connector", "test");

    assertEquals(
        ImmutableSet.of(
            ConnectorArguments.OPT_PASSWORD,
            ConnectorArguments.OPT_PRIVATE_KEY_PASSWORD,
            ConnectorArguments.OPT_IAM_SECRETACCESSKEY,
            ConnectorArguments.OPT_IAM_SESSIONTOKEN),
        JsonResponseFile.findSecretOptionNames(arguments.parser.recognizedOptions()));
  }

  private String saveResponseFile(String... args) throws IOException {
    File file = new File(temporaryFolder.getRoot(), "response.json");
    String[] allArgs =
        ObjectArrays.concat(
            args, new String[] {"--save-response-file", file.getPath()}, String.class);

    JsonResponseFile.save(new ConnectorArguments(allArgs));

    return new String(Files.readAllBytes(file.toPath()), UTF_8);
  }

  private static void assertNoneContained(String text, String... values) {
    for (String value : values) {
      assertFalse(value + " found in: " + text, text.contains(value));
    }
  }
}
