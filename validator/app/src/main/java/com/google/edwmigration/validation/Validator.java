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
package com.google.edwmigration.validation;

import com.google.cloud.storage.*;
import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem;
import com.google.common.io.Closer;
import com.google.edwmigration.validation.NameManager.ValidationType;
import com.google.edwmigration.validation.connector.Connector;
import com.google.edwmigration.validation.handle.Handle;
import com.google.edwmigration.validation.task.AbstractSourceTask;
import com.google.edwmigration.validation.task.AbstractTargetTask;
import com.google.edwmigration.validation.task.BigQuerySetupTask;
import com.google.edwmigration.validation.task.ComparisonTask;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author nehanene */
public class Validator {
  private static final Logger LOG = LoggerFactory.getLogger(Validator.class);

  private static final Pattern GCS_PATH_PATTERN =
      Pattern.compile("gs://(?<bucket>[^/]+)(?:/(?<path>.*))?/?");

  private static final String OUTPUT_DIR_PREFIX = "validationOutputs";

  public boolean run(String... args) throws Exception {
    ValidationArguments validationArguments = new ValidationArguments(args);
    return run(validationArguments);
  }

  public boolean run(@Nonnull ValidationArguments arguments) throws Exception {
    String sourceConnectionName = arguments.getSourceConnection().getConnectionType();
    String targetConnectionName = arguments.getTargetConnection().getConnectionType();
    if (sourceConnectionName == null) {
      LOG.error("Source connection type is required.");
      return false;
    } else if (targetConnectionName == null) {
      LOG.error("Target connection type is required.");
      return false;
    }

    Connector sourceConnector = ConnectorRepository.getInstance().getByName(sourceConnectionName);
    if (sourceConnector == null) {
      LOG.error(
          "Source DB '{}' not supported; available are {}.",
          sourceConnectionName,
          ConnectorRepository.getInstance().getAllNames());
      return false;
    }

    Connector targetConnector = ConnectorRepository.getInstance().getByName(targetConnectionName);
    if (targetConnector == null) {
      LOG.error(
          "Target DB '{}' not supported; available are {}.",
          targetConnectionName,
          ConnectorRepository.getInstance().getAllNames());
      return false;
    }

    return run(arguments, sourceConnector, targetConnector);
  }

  private Path prepareGcsPath(@Nonnull String gcsPath, @Nonnull Closer closer) {
    Matcher matcher = GCS_PATH_PATTERN.matcher(gcsPath);
    if (matcher.matches()) {
      String bucket = matcher.group("bucket");
      String path = matcher.group("path");
      if (path == null) {
        path = "";
      }
      LOG.debug(String.format("Setting up CloudStorageFileSystem with bucket '%s'.", bucket));
      CloudStorageFileSystem cloudStorageFileSystem =
          closer.register(CloudStorageFileSystem.forBucket(bucket));
      return cloudStorageFileSystem.getPath(path);
    } else {
      throw new IllegalArgumentException("Malformed GCS path provided: " + gcsPath);
    }
  }

  private String getFileSystemPrefix(ValidationArguments args) {
    StringBuilder sb = new StringBuilder(OUTPUT_DIR_PREFIX);
    sb.append("/");

    String database = args.getSourceConnection().getDatabase();
    if (database != null) {
      sb.append(database).append("/");
    }
    sb.append(args.getTableMapping().getSourceTable().getTable()).append("/");
    return sb.toString();
  }

  @Nonnull
  private URI prepareOutputPath(@Nonnull String outputDir, ValidationArguments args)
      throws IOException {
    Path path = Paths.get(outputDir);

    path = path.resolve(getFileSystemPrefix(args));

    if (!Files.exists(path)) {
      Files.createDirectories(path);
    }
    LOG.debug("Configured local output URI: " + path.toUri());
    return path.toUri();
  }

  private HashMap<ValidationType, String> uploadToGcs(
      Storage storage, URI outputUri, URI targetGcsUri, ValidationArguments args)
      throws IOException {
    URI targetGcsUriDir = targetGcsUri.resolve(getFileSystemPrefix(args));

    File directory = new File(outputUri);
    File[] localFiles = directory.listFiles();
    if (localFiles == null || localFiles.length == 0) {
      throw new IOException(
          String.format("No files found in path %s to upload to GCS.", outputUri));
    } else if (localFiles.length != 2) {
      throw new RuntimeException(
          String.format(
              "Expected two files per table, but found %s in directory %s",
              localFiles.length, directory.getPath()));
    }

    HashMap<ValidationType, String> targetGcsUris = new HashMap<>();

    for (File file : localFiles) {

      String fileName = file.getName();
      URI fullTargetGcsUri = targetGcsUriDir.resolve(file.getName());

      if (fileName.endsWith(AbstractSourceTask.CSV_AGGREGATE_SUFFIX)) {
        targetGcsUris.put(ValidationType.AGGREGATE, fullTargetGcsUri.toString());
      } else if (fileName.endsWith(AbstractSourceTask.CSV_ROW_SUFFIX)) {
        targetGcsUris.put(ValidationType.ROW, fullTargetGcsUri.toString());
      } else {
        throw new RuntimeException(
            String.format("Invalid file %s found in directory %s", fileName, directory.getPath()));
      }

      LOG.debug("Creating file in GCS: " + fullTargetGcsUri);

      try {
        BlobId blobId = BlobId.fromGsUtilUri(fullTargetGcsUri.toString());
        storage.createFrom(BlobInfo.newBuilder(blobId).build(), file.toPath());

        LOG.debug("Uploaded {} to {}", file.getPath(), fullTargetGcsUri);
      } catch (StorageException e) {
        LOG.error(
            "Failed to upload {} to {}: {}", file.getPath(), fullTargetGcsUri, e.getMessage());
        throw new IOException("Error uploading file", e);
      }
    }

    return targetGcsUris;
  }

  protected boolean run(
      @Nonnull ValidationArguments arguments, Connector sourceConnector, Connector targetConnector)
      throws Exception {
    LOG.info(
        "Using source connector:"
            + sourceConnector.getName()
            + " and target connector: "
            + targetConnector.getName());

    try (Closer closer = Closer.create()) {

      URI outputURI = prepareOutputPath(arguments.getOutputDir(), arguments);

      Handle sourceHandle = closer.register(sourceConnector.open(arguments.getSourceConnection()));

      AbstractSourceTask sourceQueryTask =
          sourceConnector.getSourceQueryTask(sourceHandle, outputURI, arguments);
      sourceQueryTask.run();

      ResultSetMetaData aggregateMetadata = sourceQueryTask.getAggregateQueryMetadata();
      ResultSetMetaData rowMetadata = sourceQueryTask.getRowQueryMetadata();

      Path gcsPath = prepareGcsPath(arguments.getGcsPath(), closer);
      URI gcsUri = gcsPath.toUri();
      String projectId = arguments.getProjectId();

      // BELOW BLOCK FOR RSYNC
      // Path gcsStagingBucketPath = prepareGcsPath(arguments.getGcsStagingBucket(), closer);
      // URI gcsStagingUri = gcsStagingBucketPath.toUri();
      // RSyncClient rsyncClient = new RsyncClient()
      // rsyncClient.putRsync(String projectId, outputURI, gcsStagingUri, gcsUri)

      // BELOW BLOCK FOR GCS UPLOAD
      Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
      HashMap<ValidationType, String> uploadedGcsUris =
          uploadToGcs(storage, outputURI, gcsUri, arguments);

      NameManager nameManager = new NameManager(arguments);

      BigQuerySetupTask bqAggSetup =
          new BigQuerySetupTask(
              aggregateMetadata,
              arguments.getBqStagingDataset(),
              uploadedGcsUris.get(ValidationType.AGGREGATE),
              nameManager.getBqSourceTableName(ValidationType.AGGREGATE));
      bqAggSetup.createBqExternalTable();

      BigQuerySetupTask bqRowSetup =
          new BigQuerySetupTask(
              rowMetadata,
              arguments.getBqStagingDataset(),
              uploadedGcsUris.get(ValidationType.ROW),
              nameManager.getBqSourceTableName(ValidationType.ROW));
      bqRowSetup.createBqExternalTable();

      Handle targetHandle = closer.register(targetConnector.open(arguments.getTargetConnection()));
      AbstractTargetTask targetQueryTask =
          targetConnector.getTargetQueryTask(targetHandle, nameManager, arguments);
      targetQueryTask.run();

      ComparisonTask comparisonTask = new ComparisonTask(arguments, nameManager);
      comparisonTask.run();
    }

    return true;
  }
}
