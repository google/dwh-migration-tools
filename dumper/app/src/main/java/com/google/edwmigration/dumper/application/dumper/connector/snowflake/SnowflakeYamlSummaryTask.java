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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import com.google.common.io.ByteSink;
import com.google.common.io.CharSink;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.CoreMetadataDumpFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.CoreMetadataDumpFormat.CompilerWorksDumpMetadataTaskFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.CoreMetadataDumpFormat.CompilerWorksDumpMetadataTaskFormat.Product;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.CoreMetadataDumpFormat.CompilerWorksDumpMetadataTaskFormat.Root;
import java.io.IOException;
import java.io.Writer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import org.anarres.jdiagnostics.ProductMetadata;

/** A {@link Task} that creates YAML with extraction metadata. */
@ParametersAreNonnullByDefault
abstract class SnowflakeYamlSummaryTask extends AbstractTask<Void> {

  private static final String zipEntryName = CompilerWorksDumpMetadataTaskFormat.ZIP_ENTRY_NAME;

  @Nonnull private final String zipFormat;

  @Override
  public final String describeSourceData() {
    return "containing dump metadata.";
  }

  @Override
  protected final Void doRun(@Nullable TaskRunContext context, ByteSink sink, Handle unused)
      throws IOException {
    CharSink streamSupplier = sink.asCharSink(UTF_8);
    try (Writer writer = streamSupplier.openBufferedStream()) {
      CoreMetadataDumpFormat.MAPPER.writeValue(writer, createRoot(context));
      return null;
    }
  }

  Root createRoot(@Nullable TaskRunContext context) throws IOException {
    Product product = new Product();
    product.version = String.valueOf(new ProductMetadata());
    product.arguments = serializedArguments(context);

    Root root = new Root();
    root.format = zipFormat;
    root.timestamp = System.currentTimeMillis();
    root.product = product;
    return root;
  }

  @Nonnull
  static SnowflakeYamlSummaryTask create(String zipFormat) {
    return new ContextArgumentsTask(zipFormat);
  }

  @Nonnull
  static SnowflakeYamlSummaryTask create(String zipFormat, ConnectorArguments arguments) {
    return new FixedArgumentsTask(zipFormat, arguments);
  }

  private SnowflakeYamlSummaryTask(String format) {
    super(zipEntryName);
    this.zipFormat = format;
  }

  @Nonnull
  abstract String serializedArguments(@Nullable TaskRunContext context);

  /** A task that takes arguments from {@link TaskRunContext}. */
  private static class ContextArgumentsTask extends SnowflakeYamlSummaryTask {

    ContextArgumentsTask(String zipFormat) {
      super(zipFormat);
    }

    @Override
    @Nonnull
    String serializedArguments(@Nullable TaskRunContext context) {
      context = requireNonNull(context);
      return String.valueOf(context.getArguments());
    }
  }

  /** A task that stores its own {@link ConnectorArguments}. */
  private static class FixedArgumentsTask extends SnowflakeYamlSummaryTask {

    @Nonnull final ConnectorArguments arguments;

    FixedArgumentsTask(String zipFormat, ConnectorArguments arguments) {
      super(zipFormat);
      this.arguments = arguments;
    }

    @Override
    @Nonnull
    String serializedArguments(@Nullable TaskRunContext unused) {
      return String.valueOf(arguments);
    }
  }
}
