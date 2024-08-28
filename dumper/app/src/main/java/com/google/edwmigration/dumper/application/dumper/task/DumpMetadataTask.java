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
package com.google.edwmigration.dumper.application.dumper.task;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.CoreMetadataDumpFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.anarres.jdiagnostics.ProductMetadata;

/** @author shevek */
public class DumpMetadataTask extends AbstractTask<Void>
    implements CoreMetadataDumpFormat.CompilerWorksDumpMetadataTaskFormat {

  @Nullable private final ConnectorArguments arguments;
  private final String format;

  @Nullable private final ImmutableList<String> connectors;

  public DumpMetadataTask(@Nonnull ConnectorArguments arguments, @Nonnull String format) {
    this(arguments, format, /* connectors= */ null);
  }

  public DumpMetadataTask(@Nonnull String format) {
    this(/* arguments= */ null, format, /* connectors= */ null);
  }

  private DumpMetadataTask(
      @Nullable ConnectorArguments arguments,
      @Nonnull String format,
      @Nullable List<String> connectors) {
    super(ZIP_ENTRY_NAME);
    this.arguments = arguments;
    this.format = Preconditions.checkNotNull(format, "Format was null.");
    this.connectors = connectors != null ? ImmutableList.copyOf(connectors) : null;
  }

  public static DumpMetadataTask create(
      @Nonnull ConnectorArguments arguments,
      @Nonnull String format,
      @Nonnull List<String> connectors) {
    return new DumpMetadataTask(arguments, format, connectors);
  }

  @Override
  protected Void doRun(TaskRunContext context, ByteSink sink, Handle handle) throws Exception {
    Root root = new Root();
    root.format = format;
    root.timestamp = System.currentTimeMillis();

    {
      Product product = new Product();
      product.version = String.valueOf(new ProductMetadata());
      product.arguments = String.valueOf(getArguments(context));
      product.connectors = connectors;
      root.product = product;
    }

    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      CoreMetadataDumpFormat.MAPPER.writeValue(writer, root);
    }
    return null;
  }

  private ConnectorArguments getArguments(TaskRunContext context) {
    if (this.arguments != null) {
      return this.arguments;
    }
    return context.getArguments();
  }

  @Override
  protected String describeSourceData() {
    return "containing dump metadata.";
  }
}
