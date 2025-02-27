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
package com.google.edwmigration.dumper.application.dumper.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.TeradataMetadataConnector.TeradataMetadataConnectorProperties;
import java.io.IOException;
import java.util.OptionalLong;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class PropertyParserTest {

  @Test
  public void parseNumber_success() throws IOException, MetadataDumperUsageException {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector", "teradata", "-Dteradata.metadata.max-text-length=2000");

    // Act
    OptionalLong value =
        PropertyParser.parseNumber(
            arguments,
            TeradataMetadataConnectorProperties.MAX_TEXT_LENGTH,
            Range.closed(1000L, 3000L));

    // Assert
    assertEquals(OptionalLong.of(2000), value);
  }

  @Test
  public void parseNumber_noValueSpecified() throws IOException, MetadataDumperUsageException {
    ConnectorArguments arguments = new ConnectorArguments("--connector", "teradata");

    // Act
    OptionalLong value =
        PropertyParser.parseNumber(
            arguments,
            TeradataMetadataConnectorProperties.MAX_TEXT_LENGTH,
            Range.closed(1000L, 3000L));

    // Assert
    assertEquals(OptionalLong.empty(), value);
  }

  @DataPoints("failureTestCases")
  public static final ImmutableList<TestCase> FAILURE_TEST_CASES =
      ImmutableList.of(
          TestCase.create(
              0L,
              Range.closed(3L, 4L),
              "ERROR: Option 'teradata.metadata.max-text-length' accepts only integers in range [3..4]. Actual: '0'."),
          TestCase.create(
              1L,
              Range.closed(3L, 4L),
              "ERROR: Option 'teradata.metadata.max-text-length' accepts only integers in range [3..4]. Actual: '1'."),
          TestCase.create(
              5L,
              Range.closed(3L, 4L),
              "ERROR: Option 'teradata.metadata.max-text-length' accepts only integers in range [3..4]. Actual: '5'."),
          TestCase.create(
              1L,
              Range.atLeast(2L),
              "ERROR: Option 'teradata.metadata.max-text-length' accepts only integers in range [2..+∞). Actual: '1'."));

  @Theory
  public void parseNumber_fail(@FromDataPoints("failureTestCases") TestCase testCase)
      throws IOException {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            "teradata",
            "-Dteradata.metadata.max-text-length=" + testCase.maxTextLength());

    // Act
    MetadataDumperUsageException e =
        assertThrows(
            MetadataDumperUsageException.class,
            () ->
                PropertyParser.parseNumber(
                    arguments,
                    TeradataMetadataConnectorProperties.MAX_TEXT_LENGTH,
                    testCase.allowedValues()));

    // Assert
    assertEquals(testCase.expectedErrorMessage(), e.getMessage());
  }

  @AutoValue
  abstract static class TestCase {
    abstract long maxTextLength();

    abstract Range<Long> allowedValues();

    abstract String expectedErrorMessage();

    static TestCase create(
        long maxTextLength, Range<Long> allowedValues, String expectedErrorMessage) {
      return new AutoValue_PropertyParserTest_TestCase(
          maxTextLength, allowedValues, expectedErrorMessage);
    }
  }
}
