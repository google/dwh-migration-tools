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
package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import static com.google.edwmigration.dumper.application.dumper.connector.teradata.TeradataUtils.formatQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SplitTextColumnQueryGeneratorTest {

  @Test
  public void generate_success() {
    SplitTextColumnQueryGenerator generator =
        new SplitTextColumnQueryGenerator(
            ImmutableList.of("SampleColumn"),
            "Description",
            "PartNo",
            "Corpus",
            /* whereCondition= */ Optional.empty(),
            /* textColumnOriginalLength= */ 10,
            /* splitTextColumnMaxLength= */ 5);

    // Act
    String query = generator.generate();

    // Assert
    assertEquals(
        formatQuery(
            "SELECT SampleColumn,\n"
                + " CAST(SUBSTR(Description,1,5) AS VARCHAR(5)) AS Description,\n"
                + " (PartNo - 1) * 2 + 1 AS PartNo FROM Corpus\n"
                + " UNION ALL\n"
                + " SELECT SampleColumn,\n"
                + " CAST(SUBSTR(Description,6,5) AS VARCHAR(5)) AS Description,\n"
                + " (PartNo - 1) * 2 + 2 AS PartNo FROM Corpus"),
        query);
  }

  @Test
  public void generate_splitLengthGreaterThanOriginalLength() {
    SplitTextColumnQueryGenerator generator =
        new SplitTextColumnQueryGenerator(
            ImmutableList.of("SampleColumn"),
            "Description",
            "PartNo",
            "Corpus",
            /* whereCondition= */ Optional.empty(),
            /* textColumnOriginalLength= */ 10,
            /* splitTextColumnMaxLength= */ 11);

    // Act
    String query = generator.generate();

    // Assert
    assertEquals("SELECT SampleColumn, Description, PartNo FROM Corpus", query);
  }

  @Test
  public void generate_splitLengthGreaterThanOriginalLengthWithWhereClause() {
    SplitTextColumnQueryGenerator generator =
        new SplitTextColumnQueryGenerator(
            ImmutableList.of("SampleColumn"),
            "Description",
            "PartNo",
            "Corpus",
            /* whereCondition= */ Optional.of("Description LIKE '%ABC%'"),
            /* textColumnOriginalLength= */ 10,
            /* splitTextColumnMaxLength= */ 11);

    // Act
    String query = generator.generate();

    // Assert
    assertEquals(
        "SELECT SampleColumn, Description, PartNo FROM Corpus WHERE Description LIKE '%ABC%'",
        query);
  }

  @Test
  public void generate_twoColumns() {
    SplitTextColumnQueryGenerator generator =
        new SplitTextColumnQueryGenerator(
            ImmutableList.of("SampleColumn1", "SampleColumn2"),
            "Description",
            "PartNo",
            "Corpus",
            /* whereCondition= */ Optional.empty(),
            /* textColumnOriginalLength= */ 10,
            /* splitTextColumnMaxLength= */ 5);

    // Act
    String query = generator.generate();

    // Assert
    assertEquals(
        formatQuery(
            "SELECT SampleColumn1, SampleColumn2,\n"
                + " CAST(SUBSTR(Description,1,5) AS VARCHAR(5)) AS Description,\n"
                + " (PartNo - 1) * 2 + 1 AS PartNo FROM Corpus\n"
                + " UNION ALL\n"
                + " SELECT SampleColumn1, SampleColumn2,\n"
                + " CAST(SUBSTR(Description,6,5) AS VARCHAR(5)) AS Description,\n"
                + " (PartNo - 1) * 2 + 2 AS PartNo FROM Corpus"),
        query);
  }

  @Test
  public void generate_withWhereClause() {
    SplitTextColumnQueryGenerator generator =
        new SplitTextColumnQueryGenerator(
            ImmutableList.of("SampleColumn"),
            "Description",
            "PartNo",
            "Corpus",
            /* whereCondition= */ Optional.of("Description LIKE '%DEF%'"),
            /* textColumnOriginalLength= */ 10,
            /* splitTextColumnMaxLength= */ 5);

    // Act
    String query = generator.generate();

    // Assert
    assertEquals(
        formatQuery(
            "SELECT SampleColumn, CAST(SUBSTR(Description,1,5) AS VARCHAR(5)) AS Description,\n"
                + " (PartNo - 1) * 2 + 1 AS PartNo FROM Corpus WHERE Description LIKE '%DEF%'\n"
                + " UNION ALL\n"
                + " SELECT SampleColumn,\n"
                + " CAST(SUBSTR(Description,6,5) AS VARCHAR(5)) AS Description,\n"
                + " (PartNo - 1) * 2 + 2 AS PartNo FROM Corpus WHERE Description LIKE '%DEF%'"),
        query);
  }

  @Test
  public void generate_threeParts() {
    SplitTextColumnQueryGenerator generator =
        new SplitTextColumnQueryGenerator(
            ImmutableList.of("SampleColumn"),
            "Description",
            "PartNo",
            "Corpus",
            /* whereCondition= */ Optional.empty(),
            /* textColumnOriginalLength= */ 100,
            /* splitTextColumnMaxLength= */ 40);

    // Act
    String query = generator.generate();

    // Assert
    assertEquals(
        formatQuery(
            "SELECT SampleColumn,\n"
                + " CAST(SUBSTR(Description,1,40) AS VARCHAR(40)) AS Description,\n"
                + " (PartNo - 1) * 3 + 1 AS PartNo FROM Corpus\n"
                + " UNION ALL\n"
                + " SELECT SampleColumn,\n"
                + " CAST(SUBSTR(Description,41,40) AS VARCHAR(40)) AS Description,\n"
                + " (PartNo - 1) * 3 + 2 AS PartNo FROM Corpus"
                + " UNION ALL\n"
                + " SELECT SampleColumn,\n"
                + " CAST(SUBSTR(Description,81,40) AS VARCHAR(40)) AS Description,\n"
                + " (PartNo - 1) * 3 + 3 AS PartNo FROM Corpus"),
        query);
  }

  @Test
  public void generate_originalLengthZero_fail() {
    // Act
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new SplitTextColumnQueryGenerator(
                    ImmutableList.of("SampleColumn"),
                    "Description",
                    "PartNo",
                    "Corpus",
                    /* whereCondition= */ Optional.empty(),
                    /* textColumnOriginalLength= */ 0,
                    /* splitTextColumnMaxLength= */ 5));

    // Assert
    assertEquals("textColumnOriginalLength must be greater than 0", e.getMessage());
  }

  @Test
  public void generate_splitLengthZero_fail() {
    // Act
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new SplitTextColumnQueryGenerator(
                    ImmutableList.of("SampleColumn"),
                    "Description",
                    "PartNo",
                    "Corpus",
                    /* whereCondition= */ Optional.empty(),
                    /* textColumnOriginalLength= */ 1,
                    /* splitTextColumnMaxLength= */ 0));

    // Assert
    assertEquals("splitTextColumnMaxLength must be greater than 0", e.getMessage());
  }

  @Test
  public void generate_tooManyParts_fail() {
    // Act
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new SplitTextColumnQueryGenerator(
                    ImmutableList.of("SampleColumn"),
                    "Description",
                    "PartNo",
                    "Corpus",
                    /* whereCondition= */ Optional.empty(),
                    /* textColumnOriginalLength= */ 11,
                    /* splitTextColumnMaxLength= */ 1));

    // Assert
    assertEquals(
        "Too many parts after splitting. Original length='11', splitTextColumnMaxLength='1',"
            + " parts count='11', max parts count='10'.",
        e.getMessage());
  }
}
