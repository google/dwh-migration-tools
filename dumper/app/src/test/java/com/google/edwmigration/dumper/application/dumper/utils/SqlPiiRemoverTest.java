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
package com.google.edwmigration.dumper.application.dumper.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class SqlPiiRemoverTest {

  @Test
  public void remove_removesSimpleLiterals() {
    String sql = "SELECT * FROM test_table WHERE value = 'Secret'";
    String expected = "SELECT * FROM test_table WHERE value = '-'";

    // Act
    String result = SqlPiiRemover.remove(sql);

    // Assert
    assertEquals(expected, result);
  }

  @Test
  public void remove_removesSpecialCharacters() {
    String sql =
        "SELECT * FROM test_table WHERE value = 'Secret Which has # & \n"
            + " --newline and \', really??! '";
    String expected = "SELECT * FROM test_table WHERE value = '-'";

    // Act
    String result = SqlPiiRemover.remove(sql);

    // Assert
    assertEquals(expected, result);
  }

  @Test
  public void removeComment_removesCommentAtEndOfQuery() {
    String sql = "SELECT * FROM test_table -- Test Comment";
    String expected = "SELECT * FROM test_table ";

    // Act
    String result = SqlPiiRemover.remove(sql);

    // Assert
    assertEquals(expected, result);
  }

  @Test
  public void remove_removesCommentInMiddleOfMultilineQuery() {
    String sql = "SELECT \ntest_column -- test comment\n FROM test_table";
    String expected = "SELECT \ntest_column \n FROM test_table";

    // Act
    String result = SqlPiiRemover.remove(sql);

    // Assert
    assertEquals(expected, result);
  }

  @Test
  public void remove_removesCommentInFullLine() {
    String sql = "SELECT \ntest_column \n-- the whole line is a comment\n FROM test_table";
    String expected = "SELECT \ntest_column \n\n FROM test_table";

    // Act
    String result = SqlPiiRemover.remove(sql);

    // Assert
    assertEquals(expected, result);
  }

  @Test
  public void remove_removesMultipleComments() {
    String sql =
        "SELECT \n"
            + "test_column \n"
            + "-- the whole line is a comment\n"
            + " FROM test_table -- and another comment";
    String expected = "SELECT \ntest_column \n\n FROM test_table ";

    // Act
    String result = SqlPiiRemover.remove(sql);

    // Assert
    assertEquals(expected, result);
  }

  @Test
  public void remove_removesAsterixCommentInsideALine() {
    String sql = "SELECT * /* this is the comment */ FROM test_table";
    String expected = "SELECT *  FROM test_table";

    // Act
    String result = SqlPiiRemover.remove(sql);

    // Assert
    assertEquals(expected, result);
  }

  @Test
  public void remove_removesMultilineComment() {
    String sql = "SELECT * /* this is a\n multiline comment */ FROM test_table";
    String expected = "SELECT *  FROM test_table";

    // Act
    String result = SqlPiiRemover.remove(sql);

    // Assert
    assertEquals(expected, result);
  }
}
