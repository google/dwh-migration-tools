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
package com.google.edwmigration.dumper.application.dumper;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** */
@RunWith(JUnit4.class)
public class JsonResponseFileTest {

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
}
