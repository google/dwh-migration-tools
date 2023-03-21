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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/** And that's how .. dumper got emitter.... */
public class SqlBuilder {

  private final List<String> projections = new ArrayList<>();
  private final List<String> orderBy = new ArrayList<>();
  private String fromTable = null;
  private final List<String> whereClause = new ArrayList<>();

  @Nonnull
  public SqlBuilder withProjections(@Nonnull String... proj) {
    Collections.addAll(projections, proj);
    return this;
  }

  @Nonnull
  public SqlBuilder withFromTable(@Nonnull String fromTable) {
    this.fromTable = Preconditions.checkNotNull(fromTable, "Table name was null.");
    return this;
  }

  @Nonnull
  public SqlBuilder withWhereInVals(@Nonnull String id, @CheckForNull List<? extends String> vals) {
    Preconditions.checkArgument(id != null, "id was null.");
    if (vals != null && !vals.isEmpty()) {
      StringBuilder buf = new StringBuilder();
      buf.append(id).append(" IN (");
      Joiner.on(", ").appendTo(buf, Lists.transform(vals, s -> "'" + s + "'"));
      buf.append(")");
      whereClause.add(buf.toString());
    }
    return this;
  }

  @Nonnull
  public SqlBuilder withOrderBy(@Nonnull String... orderBy) {
    Collections.addAll(this.orderBy, orderBy);
    return this;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("SELECT");

    //  1. PROJECTIONS
    if (projections.isEmpty()) sb.append(" *");
    else Joiner.on(", ").appendTo(sb.append(' '), projections);

    // 2. FROM
    sb.append(" FROM ").append(fromTable);

    // 3. WHERE  CLAUSES
    if (!whereClause.isEmpty()) {
      sb.append(" WHERE ");
      Joiner.on(" AND ").appendTo(sb, whereClause);
    }

    // 4. ORDER BY
    if (!orderBy.isEmpty()) {
      sb.append(" ORDER BY ");
      Joiner.on(", ").appendTo(sb, orderBy);
    }

    return sb.toString();
  }

  // a quick hack for only toWhereClause .
  // to get little more cool-off time before changing all the selects.
  @Nonnull
  public String toWhereClause() {
    if (whereClause.isEmpty()) return "";

    StringBuilder sb = new StringBuilder();
    sb.append(" WHERE ");
    Joiner.on(" AND ").appendTo(sb, whereClause);

    return sb.toString();
  }
}
