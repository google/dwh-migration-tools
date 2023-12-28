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
package com.google.edwmigration.dumper.application.dumper.connector.redshift;

import com.amazonaws.services.redshift.AmazonRedshift;
import com.amazonaws.services.redshift.AmazonRedshiftClientBuilder;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/** Abstract class that provides methods for connecting with AWS API and writing results. */
public abstract class AbstractAwsApiTask extends AbstractTask<Void> {

  Class<? extends Enum<?>> headerEnum;
  AmazonRedshift redshiftClient;

  public AbstractAwsApiTask(String zipEntryName, Class<? extends Enum<?>> headerEnum) {
    super(zipEntryName);
    this.headerEnum = headerEnum;
    this.redshiftClient = AmazonRedshiftClientBuilder.standard().build();
  }

  @Nonnull
  public AbstractAwsApiTask withRedshiftApiClient(AmazonRedshift redshiftClient) {
    this.redshiftClient = redshiftClient;
    return this;
  }

  public AmazonRedshift redshiftApiClient() {
    return redshiftClient;
  }

  public Void writeRecordsCsv(@Nonnull ByteSink sink, Stream<Object[]> records) throws IOException {
    CSVFormat format = FORMAT.builder().setHeader(headerEnum).build();
    try (RecordProgressMonitor monitor = new RecordProgressMonitor(getName());
        Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      CSVPrinter printer = format.print(writer);

      records.forEach(
          record -> {
            try {
              monitor.count();
              printer.printRecord(record);
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });
    }
    return null;
  }
}
