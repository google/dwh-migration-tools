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
package com.google.edwmigration.validation.io;

import com.google.cloud.storage.*;
import com.google.edwmigration.validation.config.ValidationType;
import com.google.edwmigration.validation.io.writer.CsvWriter;
import com.google.edwmigration.validation.model.Either;
import com.google.edwmigration.validation.model.Failure;
import com.google.edwmigration.validation.model.UserInputContext;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for uploading local output files to GCS and resolving appropriate destination paths.
 */
public class GcsUpload {

  private static final Logger LOG = LoggerFactory.getLogger(GcsUpload.class);

  private static final Pattern GCS_PATH_PATTERN =
      Pattern.compile("gs://(?<bucket>[^/]+)(?:/(?<path>.*))?/?");

  public static String getFileSystemPrefix(UserInputContext context) {
    return String.format(
        "validationOutputs/%s/%s/", context.sourceConnection.database, context.sourceTable.name);
  }

  /**
   * Parses a GCS URI string into its bucket and path components.
   *
   * <p>Example: parseGcsUri("gs://my-bucket/path/to/data") returns ParsedGcsUri("my-bucket",
   * "path/to/data/")
   *
   * @param gcsUriString A GCS URI of the form gs://bucket[/optional/path]
   * @return A ParsedGcsUri object containing the bucket name and normalized base path.
   */
  public static ParsedGcsUri parseGcsUri(String gcsUriString) {
    Matcher matcher = GCS_PATH_PATTERN.matcher(gcsUriString);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Malformed GCS path: " + gcsUriString);
    }

    String bucket = matcher.group("bucket");
    // The path part of the URI, or empty if not present.
    String basePath = matcher.group("path") != null ? matcher.group("path") : "";
    // Ensure the basePath ends with a slash, so GCS knows it's a directory.
    if (!basePath.endsWith("/")) basePath += "/";

    return new ParsedGcsUri(bucket, basePath);
  }

  /**
   * Uploads the expected aggregate and row CSV output files to a resolved GCS location.
   *
   * @param storage The GCS Storage client.
   * @param localOutputDir The local directory containing output CSVs.
   * @param gcsUriString The destination GCS path (e.g., gs://bucket/path).
   * @param context The full user input context (used for path resolution).
   * @return Either a map of ValidationType to uploaded GCS URIs, or a descriptive error string.
   */
  public static Either<Failure, HashMap<ValidationType, String>> uploadToGcs(
      Storage storage, URI localOutputDir, String gcsUriString, UserInputContext context) {

    File localDir = new File(localOutputDir);
    if (!localDir.exists() || !localDir.isDirectory()) {
      return Either.left(
          Failure.CONFIG.with("Provided output path is not a valid directory: " + localOutputDir));
    }

    File[] files = localDir.listFiles();
    if (files == null || files.length == 0) {
      return Either.left(
          Failure.CONFIG.with(
              String.format("No files found in path %s to upload to GCS.", localOutputDir)));
    }

    File aggFile = null;
    File rowFile = null;
    for (File file : files) {
      if (file.getName().endsWith(CsvWriter.CSV_AGGREGATE_SUFFIX)) {
        aggFile = file;
      } else if (file.getName().endsWith(CsvWriter.CSV_ROW_SUFFIX)) {
        rowFile = file;
      }
    }

    if (aggFile == null)
      return Either.left(Failure.GCS_UPLOAD.with("Aggregate file not found in " + localOutputDir));
    if (rowFile == null)
      return Either.left(Failure.GCS_UPLOAD.with("Row file not found in " + localOutputDir));

    ParsedGcsUri parsed = parseGcsUri(gcsUriString);
    String targetPrefix = parsed.basePath + getFileSystemPrefix(context);

    HashMap<ValidationType, String> resultUris = new HashMap<>();

    try {
      uploadFileToGcs(
          storage, aggFile, targetPrefix, parsed.bucket, ValidationType.AGGREGATE, resultUris);
      uploadFileToGcs(
          storage, rowFile, targetPrefix, parsed.bucket, ValidationType.ROW, resultUris);
    } catch (IOException e) {
      return Either.left(Failure.GCS_UPLOAD.with("Upload to GCS failed: " + e.getMessage(), e));
    }

    return Either.right(resultUris);
  }

  private static void uploadFileToGcs(
      Storage storage,
      File file,
      String targetPrefix,
      String bucket,
      ValidationType type,
      HashMap<ValidationType, String> resultUris)
      throws IOException {
    String objectPath = targetPrefix + file.getName();
    BlobId blobId = BlobId.of(bucket, objectPath);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.createFrom(blobInfo, file.toPath());
    String fullUri = String.format("gs://%s/%s", bucket, objectPath);
    resultUris.put(type, fullUri);
    LOG.debug("Uploaded {} to {}", file.getName(), fullUri);
  }

  /** Parsed components of a GCS URI. */
  public static class ParsedGcsUri {
    public final String bucket;
    public final String basePath;

    public ParsedGcsUri(String bucket, String basePath) {
      this.bucket = bucket;
      this.basePath = basePath;
    }
  }
}
