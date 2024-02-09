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

import static com.google.edwmigration.dumper.application.dumper.utils.OptionalUtils.optionalIf;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.redshift.AmazonRedshift;
import com.amazonaws.services.redshift.AmazonRedshiftClient;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/** Abstract class that provides methods for connecting with AWS API and writing results. */
public abstract class AbstractAwsApiTask extends AbstractTask<Void> {

  AWSCredentialsProvider credentialsProvider;
  Class<? extends Enum<?>> headerEnum;
  Optional<AmazonRedshift> redshiftClient;
  Optional<AmazonCloudWatch> cloudWatchClient;

  public AbstractAwsApiTask(
      AWSCredentialsProvider credentialsProvider,
      String zipEntryName,
      Class<? extends Enum<?>> headerEnum) {
    super(zipEntryName);
    this.headerEnum = headerEnum;
    this.redshiftClient = Optional.empty();
    this.cloudWatchClient = Optional.empty();
    this.credentialsProvider = credentialsProvider;
  }

  @Nonnull
  public AbstractAwsApiTask withRedshiftApiClient(AmazonRedshift redshiftClient) {
    this.redshiftClient = Optional.of(redshiftClient);
    return this;
  }

  @Nonnull
  public AbstractAwsApiTask withCloudWatchApiClient(AmazonCloudWatch cloudWatchClient) {
    this.cloudWatchClient = Optional.of(cloudWatchClient);
    return this;
  }

  public AmazonRedshift redshiftApiClient() {
    return redshiftClient.orElseGet(
        () -> AmazonRedshiftClient.builder().withCredentials(credentialsProvider).build());
  }

  public AmazonCloudWatch cloudWatchApiClient() {
    return cloudWatchClient.orElseGet(
        () -> AmazonCloudWatchClient.builder().withCredentials(credentialsProvider).build());
  }

  public void writeRecordsCsv(@Nonnull ByteSink sink, List<Object[]> records) throws IOException {
    CSVFormat format = FORMAT.builder().setHeader(headerEnum).build();
    try (RecordProgressMonitor monitor = new RecordProgressMonitor(getName());
        Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      CSVPrinter printer = format.print(writer);

      for (Object[] record : records) {
        monitor.count();
        printer.printRecord(record);
      }
    }
  }

  public static Optional<AWSCredentialsProvider> createCredentialsProvider(
      ConnectorArguments arguments) {
    String profileName = arguments.getIAMProfile();
    if (profileName != null) {
      return Optional.of(new ProfileCredentialsProvider(profileName));
    }

    String iamAccessKey = arguments.getIAMAccessKeyID();
    String iamSecretAccessKey = arguments.getIAMSecretAccessKey();

    return optionalIf(
        iamAccessKey != null && iamSecretAccessKey != null,
        () ->
            new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(iamAccessKey, iamSecretAccessKey)));
  }
}
