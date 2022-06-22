/*
 * Copyright 2022 Google LLC
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
package com.google.edwmigration.dumper.sql;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.io.Resources;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A helper class for reading .sql files. */
public final class SqlUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlUtil.class);

  private SqlUtil() {}

  /**
   * @param sqlPath Path to an .sql file.
   * @return File contents, never null.
   */
  public static String getSql(String sqlPath) throws IOException {
    return Resources.toString(Resources.getResource(sqlPath), UTF_8);
  }

}
