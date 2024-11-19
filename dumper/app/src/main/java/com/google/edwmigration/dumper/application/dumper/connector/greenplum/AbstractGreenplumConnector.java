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
package com.google.edwmigration.dumper.application.dumper.connector.greenplum;

import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDriver;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDriverClass;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentHostUnlessUrl;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentJDBCUri;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentPassword;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentUser;
import com.google.edwmigration.dumper.application.dumper.connector.postgresql.AbstractPostgresqlConnector;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import javax.annotation.Nonnull;

/** @author zzwang */
@RespectsArgumentDriver
@RespectsArgumentDriverClass
@RespectsArgumentHostUnlessUrl
@RespectsArgumentUser
@RespectsArgumentPassword
@RespectsArgumentJDBCUri
public abstract class AbstractGreenplumConnector extends AbstractPostgresqlConnector {

  protected static final DateTimeFormatter SQL_FORMAT =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC);

  public AbstractGreenplumConnector(String name) {
    super(name);
  }

  @Nonnull
  protected static CharSequence newWhereClause(String... clauseArray) {
    StringBuilder buf = new StringBuilder();
    for (String clause : clauseArray) {
      if (buf.length() > 0) buf.append(" AND ");
      buf.append(clause);
    }
    return buf.toString();
  }
}
