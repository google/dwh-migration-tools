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
package com.google.edwmigration.validation.application.validator;

import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem;
import com.google.common.io.Closer;
import com.google.edwmigration.validation.application.validator.connector.Connector;
import com.google.edwmigration.validation.application.validator.handle.Handle;
import com.google.edwmigration.validation.application.validator.task.AbstractTask;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author nehanene */
public class Validator {
  private static final Logger LOG = LoggerFactory.getLogger(Validator.class);

  private static final Pattern GCS_PATH_PATTERN =
      Pattern.compile("gs://(?<bucket>[^/]+)/(?<path>.*)/?");

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
      LOG.debug(
          String.format(
              "Setting up CloudStorageFileSystem with bucket '%s' and path '%s'.", bucket, path));
      CloudStorageFileSystem cloudStorageFileSystem =
          closer.register(CloudStorageFileSystem.forBucket(bucket));
      return cloudStorageFileSystem.getPath(path);
    } else {
      throw new IllegalArgumentException("Malformed GCS path provided: " + gcsPath);
    }
  }

  @Nonnull
  private Path prepareOutputPath(@Nonnull String outputDir, ValidationArguments args)
      throws IOException {
    String db = args.getSourceConnection().getDatabase();
    Path path = Paths.get(outputDir);

    if (db != null) {
      path = path.resolve(db);
    }

    if (!Files.exists(path)) {
      Files.createDirectories(path);
    }
    return path;
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

      Path outputPath = prepareOutputPath(arguments.getOutputDir(), arguments);
      URI outputURI = outputPath.toUri();

      Handle sourceHandle = closer.register(sourceConnector.open(arguments.getSourceConnection()));
      //      new ExportJob(sourceHandle, outputUri, arguments).run();

      AbstractTask sourceQueryTask =
          sourceConnector.getSourceQueryTask(sourceHandle, outputURI, arguments);
      sourceQueryTask.run();

      Path gcsPath = prepareGcsPath(arguments.getGcsPath(), closer);
      URI gcsUri = gcsPath.toUri();

      Path gcsStagingBucketPath = prepareGcsPath(arguments.getGcsStagingBucket(), closer);
      URI gcsStagingUri = gcsStagingBucketPath.toUri();

      String projectId = arguments.getProjectId();

      // RSyncClient rsyncClient = new RsyncClient()
      // rsyncClient.putRsync(String projectId, outputURI, gcsStagingUri, gcsUri)

      // generate external table

      // TargetQueryTask targetQueryTask = targetConnector.getTargetQueryTask(targetHandle,
      // externalTableId, arguments);
      // targetQuery.run()

      // run comparison query

    }

    return true;
  }
}
