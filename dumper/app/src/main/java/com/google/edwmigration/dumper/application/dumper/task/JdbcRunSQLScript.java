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
package com.google.edwmigration.dumper.application.dumper.task;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import java.io.File;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import javax.annotation.Nonnull;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

/** */
public class JdbcRunSQLScript extends AbstractTask<Void> {

  private final File sqlScript;

  public JdbcRunSQLScript(@Nonnull File script) {
    super(script.getName());
    sqlScript = Preconditions.checkNotNull(script);
  }

  @Override
  protected Void doRun(TaskRunContext context, ByteSink sink, Handle handle) throws Exception {
    JdbcHandle jdbcHandle = (JdbcHandle) handle;

    FileSystemResource resource = new FileSystemResource(sqlScript);
    ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator(resource);
    jdbcHandle
        .getJdbcTemplate()
        .execute(
            (Connection conn) -> {
              databasePopulator.populate(conn);
              return null;
            });

    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      writer.append("-- Successfully executed:\n\n");
      writer.append(new String(Files.readAllBytes(sqlScript.toPath()), StandardCharsets.UTF_8));
    }
    return null;
  }
}
