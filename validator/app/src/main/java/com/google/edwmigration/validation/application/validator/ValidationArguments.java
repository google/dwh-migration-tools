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

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import joptsimple.OptionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author nehanene */
public class ValidationArguments extends DefaultArguments {

  private static final Logger LOG = LoggerFactory.getLogger(ValidationArguments.class);
  public static final String OPT_SOURCE_CONNECTION = "source-connection";
  public static final String OPT_TARGET_CONNECTION = "target-connection";
  public static final String OPT_OUTPUT = "output";
  public static final String OPT_GCS_PATH = "gcs-path";
  public static final String OPT_CONFIDENCE_INTERVAL = "confidence-interval";
  public static final String OPT_TABLE = "table";
  public static final String OPT_COLUMN_MAPPINGS = "column-mappings";
  public static final String OPT_PROJECT_ID = "project-id";
  public static final String OPT_GCS_STAGING_BUCKET = "gcs-staging-bucket";

  private ValidationConnection sourceConnection;
  private ValidationConnection targetConnection;
  private final OptionSpec<String> sourceConnectionOption =
      parser
          .accepts(OPT_SOURCE_CONNECTION, "Source connection file.")
          .withRequiredArg()
          .ofType(String.class)
          .describedAs("path/to/teradata.json")
          .required();

  private final OptionSpec<String> targetConnectionOption =
      parser
          .accepts(OPT_TARGET_CONNECTION, "Target connection file.")
          .withRequiredArg()
          .ofType(String.class)
          .describedAs("path/to/bigquery.json")
          .required();

  private final OptionSpec<String> tableOption =
      parser
          .accepts(OPT_TABLE, "Table to validate.")
          .withRequiredArg()
          .ofType(String.class)
          .describedAs("source=target")
          .required();

  private final OptionSpec<String> outputOption =
      parser
          .accepts(
              OPT_OUTPUT,
              "Output directory to export query results. Defaults to current directory.")
          .withOptionalArg()
          .ofType(String.class)
          .describedAs("path/to/dir")
          .defaultsTo("validationOutputs/");

  private final OptionSpec<String> gcsPathOption =
      parser
          .accepts(OPT_GCS_PATH, "GCS URI to upload query results to.")
          .withRequiredArg()
          .ofType(String.class)
          .describedAs("gs://path/to/dir");

  private final OptionSpec<Double> confidenceIntervalOption =
      parser
          .accepts(
              OPT_CONFIDENCE_INTERVAL,
              "Confidence interval for validation. Determines number of rows sampled. Defaults to 90.")
          .withOptionalArg()
          .ofType(Double.class)
          .describedAs("0.90")
          .defaultsTo(0.90);

  private final OptionSpec<String> columnMappingsOption =
      parser
          .accepts(OPT_COLUMN_MAPPINGS, "Column name mappings.")
          .withOptionalArg()
          .ofType(String.class)
          .withValuesSeparatedBy(',')
          .describedAs("colA=COLA,colB=newColB");

  private final OptionSpec<String> projectIdOption =
      parser
          .accepts(OPT_PROJECT_ID, "Project ID for Rsync.")
          .withRequiredArg()
          .ofType(String.class)
          .describedAs("projectId");

  private final OptionSpec<String> GcsStagingBucketOption =
      parser
          .accepts(
              OPT_GCS_STAGING_BUCKET,
              "Staging bucket for Rysnc. Defaults to the same bucket as GCS path.")
          .withOptionalArg()
          .ofType(String.class)
          .describedAs("gs://mybucket")
          .defaultsTo(getGcsPath());

  public static final String DEFAULT_ENV_DIRECTORY = "~/.config/dwh-validation/";
  public static final String ENV_DIRECTORY_VAR = "DV_CONN_HOME";
  String connectionsDir =
      Optional.ofNullable(System.getenv(ENV_DIRECTORY_VAR)).orElse(DEFAULT_ENV_DIRECTORY);
  Gson gson = new Gson();

  public Path getSourceConnectionPath() {
    Path sourceConnPath = Paths.get(getOptions().valueOf(sourceConnectionOption));
    return Paths.get(connectionsDir).resolve(sourceConnPath);
  }

  public ValidationConnection getSourceConnection() {
    if (sourceConnection == null) {
      try {
        String sourceConnJson = new String(Files.readAllBytes(getSourceConnectionPath()));
        sourceConnection = gson.fromJson(sourceConnJson, ValidationConnection.class);
      } catch (IOException e) {
        throw new IllegalArgumentException(
            String.format("An invalid file was provided: '%s'.", getSourceConnectionPath()), e);
      }
    }
    return sourceConnection;
  }

  public Path getTargetConnectionPath() {
    Path targetConnPath = Paths.get(getOptions().valueOf(targetConnectionOption));
    return Paths.get(connectionsDir).resolve(targetConnPath);
  }

  public ValidationConnection getTargetConnection() {
    if (targetConnection == null) {
      try {
        String targetConnJson = new String(Files.readAllBytes(getTargetConnectionPath()));
        targetConnection = gson.fromJson(targetConnJson, ValidationConnection.class);
      } catch (IOException e) {
        throw new IllegalArgumentException(
            String.format("An invalid file was provided: '%s'.", getTargetConnectionPath()), e);
      }
    }
    return targetConnection;
  }

  public String getOutputDir() {
    return getOptions().valueOf(outputOption);
  }

  public String getGcsPath() {
    return getOptions().valueOf(gcsPathOption);
  }

  public String getProjectId() {
    return getOptions().valueOf(projectIdOption);
  }

  public Double getOptConfidenceInterval() {
    return getOptions().valueOf(confidenceIntervalOption);
  }

  public String getTable() {
    return getOptions().valueOf(tableOption);
  }

  public String getGcsStagingBucket() {
    return getOptions().valueOf(GcsStagingBucketOption);
  }

  public ImmutableMap<String, String> getColumnMappings() {
    List<String> mappings = getOptions().valuesOf(columnMappingsOption);
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (String pair : mappings) {
      String[] parts = pair.split("=");
      if (parts.length == 2) {
        builder.put(parts[0], parts[1]);
      } else {
        throw new IllegalArgumentException("Invalid column mapping format found: " + pair);
      }
    }

    return builder.build();
  }

  public ValidationArguments(@Nonnull String... args) {
    super(args);
  }
}
