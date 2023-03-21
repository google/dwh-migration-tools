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
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.MetadataDumperConstants;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;

/** @author shevek */
@Deprecated // Use DumpMetadataTask
public class ArgumentsTask extends AbstractTask<Void> {

  @Nonnull private final ConnectorArguments arguments;

  public ArgumentsTask(String targetPath, @Nonnull ConnectorArguments arguments) {
    super(targetPath);
    this.arguments = Preconditions.checkNotNull(arguments, "Arguments was null.");
  }

  public ArgumentsTask(@Nonnull final ConnectorArguments arguments) {
    this(MetadataDumperConstants.ARGUMENTS_ZIP_ENTRY_NAME, arguments);
  }

  @Override
  public TaskCategory getCategory() {
    return TaskCategory.INFORMATIONAL;
  }

  @Override
  protected Void doRun(TaskRunContext context, ByteSink sink, Handle handle) throws Exception {
    sink.asCharSink(StandardCharsets.UTF_8).write(arguments.toString());
    return null;
  }

  @Override
  public String toString() {
    return "Write " + getTargetPath() + " from command-line arguments.";
  }
}
