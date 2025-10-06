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
package com.google.edwmigration.dumper.application.dumper.connector.redshift;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.redshift.AmazonRedshift;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/** Abstract class that provides methods for connecting with AWS API and writing results. */
public abstract class AbstractAwsApiTask extends AbstractTask<Void> {

  Class<? extends Enum<?>> headerEnum;
  AmazonRedshift redshiftClient;
  AmazonCloudWatch cloudWatchClient;

  public AbstractAwsApiTask(
      AmazonRedshift redshiftClient, String zipEntryName, Class<? extends Enum<?>> headerEnum) {
    super(zipEntryName);
    this.redshiftClient = redshiftClient;
    this.headerEnum = headerEnum;
  }

  public AbstractAwsApiTask(
      AmazonRedshift redshiftClient,
      AmazonCloudWatch amazonCloudWatch,
      String zipEntryName,
      Class<? extends Enum<?>> headerEnum) {
    super(zipEntryName);
    this.headerEnum = headerEnum;
    this.redshiftClient = redshiftClient;
    this.cloudWatchClient = amazonCloudWatch;
  }

  static class CsvRecordWriter implements AutoCloseable {
    private final CSVPrinter printer;
    private final RecordProgressMonitor monitor;
    private final Writer writer;

    CsvRecordWriter(ByteSink sink, CSVFormat format, String name) throws IOException {
      monitor = new RecordProgressMonitor(name);
      writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
      printer = format.print(writer);
    }

    void handleRecord(Object... record) throws IOException {
      monitor.count();
      printer.printRecord(record);
    }

    @Override
    public void close() throws IOException {
      // close Monitor first, because closing Writer can throw a checked exception
      monitor.close();
      writer.close();
    }
  }

  public static Optional<AWSCredentialsProvider> createCredentialsProvider(
      ConnectorArguments arguments) {
    return Optional.ofNullable(doCreateProvider(arguments));
  }

  @Nullable
  private static AWSCredentialsProvider doCreateProvider(ConnectorArguments arguments) {
    String profileName = arguments.getIAMProfile();
    if (profileName != null) {
      return new ProfileCredentialsProvider(profileName);
    }
    String accessKeyId = arguments.getIAMAccessKeyID();
    String secretAccessKey = arguments.getIAMSecretAccessKey();
    String sessionToken = arguments.getIamSessionToken();
    if (accessKeyId == null || secretAccessKey == null) {
      return null;
    }

    if (sessionToken != null) {
      BasicSessionCredentials credentials =
          new BasicSessionCredentials(accessKeyId, secretAccessKey, sessionToken);
      return new AWSStaticCredentialsProvider(credentials);
    }
    BasicAWSCredentials credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
    return new AWSStaticCredentialsProvider(credentials);
  }
}
