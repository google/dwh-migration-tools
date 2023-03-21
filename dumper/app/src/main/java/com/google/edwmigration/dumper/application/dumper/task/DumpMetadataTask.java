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
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.CoreMetadataDumpFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import org.anarres.jdiagnostics.ProductMetadata;

/** @author shevek */
public class DumpMetadataTask extends AbstractTask<Void>
    implements CoreMetadataDumpFormat.CompilerWorksDumpMetadataTaskFormat {

  @Nonnull private final ConnectorArguments arguments;
  private final String format;

  public DumpMetadataTask(@Nonnull ConnectorArguments arguments, @Nonnull String format) {
    super(ZIP_ENTRY_NAME);
    this.arguments = Preconditions.checkNotNull(arguments, "Arguments was null.");
    this.format = Preconditions.checkNotNull(format, "Format was null.");
  }

  @Override
  protected Void doRun(TaskRunContext context, ByteSink sink, Handle handle) throws Exception {
    Root root = new Root();
    root.format = format;
    root.timestamp = System.currentTimeMillis();

    {
      Product product = new Product();
      product.version = String.valueOf(new ProductMetadata());
      product.arguments = String.valueOf(arguments);
      root.product = product;
    }

    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      CoreMetadataDumpFormat.MAPPER.writeValue(writer, root);
    }
    return null;
  }

  @Override
  public String toString() {
    return "Write " + getTargetPath() + " containing dump metadata.";
  }
}
