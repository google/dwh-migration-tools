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
package com.google.edwmigration.dumper.application.dumper.clouddumper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.FileEntity;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Retriever for JDBC drivers. */
public class DriverRetriever {

  private static final Logger LOG = LoggerFactory.getLogger(DriverRetriever.class);

  private static final BaseEncoding HEX_ENCODER = BaseEncoding.base16().lowerCase();

  private final CloseableHttpClient httpClient;
  private final Path driverPath;
  private final ImmutableMap<String, DriverInformation> driverInformationMap;

  @VisibleForTesting
  DriverRetriever(
      CloseableHttpClient httpClient,
      Path driverPath,
      ImmutableList<DriverInformation> driverInformationList) {
    this.httpClient = httpClient;
    this.driverPath = driverPath;
    this.driverInformationMap =
        driverInformationList.stream()
            .flatMap(
                driverInformation ->
                    Stream.concat(
                        Stream.of(Pair.of(driverInformation.name(), driverInformation)),
                        driverInformation.aliases().stream()
                            .map(alias -> Pair.of(alias, driverInformation))))
            .collect(ImmutableMap.toImmutableMap(Pair::getKey, Pair::getValue));
  }

  public static DriverRetriever create(CloseableHttpClient httpClient, Path driverPath) {
    ImmutableList<DriverInformation> driverInformationList;
    try {
      driverInformationList =
          ImmutableList.of(
              DriverInformation.builder(
                      "redshift",
                      new URI(
                          "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/2.1.0.17/redshift-jdbc42-2.1.0.17.zip"))
                  .setAliases("redshift-logs", "redshift-raw-logs")
                  .setChecksum(
                      HEX_ENCODER.decode(
                          "258c2e193e1d70ef637e5a0db08256dcf68c53ace87d0627e3c81339d8d6a449"))
                  .build(),
              DriverInformation.builder(
                      "bigquery",
                      new URI(
                          "https://storage.googleapis.com/simba-bq-release/jdbc/SimbaJDBCDriverforGoogleBigQuery42_1.3.3.1004.zip"))
                  .setAliases("bigquery-logs")
                  .setChecksum(
                      HEX_ENCODER.decode(
                          "51eb2186a167f76671546cf98f612a1e772a20c4db867d986b0b1fe7f8acfffb"))
                  .build(),
              DriverInformation.builder(
                      "snowflake",
                      new URI(
                          "https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.33/snowflake-jdbc-3.13.33.jar"))
                  .setAliases(
                      "snowflake-account-usage-logs",
                      "snowflake-account-usage-metadata",
                      "snowflake-information-schema-logs",
                      "snowflake-information-schema-metadata",
                      "snowflake-logs")
                  .setChecksum(
                      HEX_ENCODER.decode(
                          "84f60f5c2d53f7cbc03ecb6ebf6f78b0e626f450cc7adeb49123959acc440811"))
                  .build(),
              DriverInformation.builder(
                      "hive",
                      new URI(
                          "https://repo1.maven.org/maven2/org/apache/hive/hive-jdbc/3.1.3/hive-jdbc-3.1.3.jar"))
                  .setChecksum(
                      HEX_ENCODER.decode(
                          "f9fa451cf20598013df335e4328b9580fdec0c3fd72d7149dd2ceadb043be7c6"))
                  .build());
    } catch (URISyntaxException e) {
      throw new RuntimeException("Got unexpected exception.", e);
    }
    return new DriverRetriever(httpClient, driverPath, driverInformationList);
  }

  /**
   * Gets the driver for the given database (e.g. "teradata") and returns the path to it if
   * available.
   */
  public Optional<Path> getDriver(String name) throws IOException {
    if (!driverInformationMap.containsKey(name)) {
      LOG.info("Got no driver for connector '{}'.", name);
      return Optional.empty();
    }
    DriverInformation driverInformation = driverInformationMap.get(name);
    ClassicHttpRequest httpGet = ClassicRequestBuilder.get(driverInformation.uri()).build();
    return httpClient.execute(
        httpGet,
        response -> {
          Preconditions.checkState(
              response.getCode() == HttpStatus.SC_OK,
              "Failed to get driver for '%s': Got unexpected response code %s for URI '%s'.",
              name,
              response.getCode(),
              driverInformation.uri());
          Path outputPath = driverPath.resolve(driverInformation.getDriverFileName());
          try (OutputStream output = Files.newOutputStream(outputPath)) {
            FileEntity.writeTo(response.getEntity(), output);
          }
          checkChecksum(driverInformation, outputPath);
          return Optional.of(outputPath);
        });
  }

  private void checkChecksum(DriverInformation driverInformation, Path outputPath)
      throws IOException {
    if (driverInformation.checksum().isPresent()) {
      byte[] checksum = driverInformation.checksum().get();
      HashCode hashCode =
          com.google.common.io.Files.asByteSource(outputPath.toFile()).hash(Hashing.sha256());
      Preconditions.checkState(
          Arrays.equals(checksum, hashCode.asBytes()),
          "Retrieved driver for %s expected to have SHA256 sum '%s' but got '%s'.",
          driverInformation.name(),
          HEX_ENCODER.encode(checksum),
          HEX_ENCODER.encode(hashCode.asBytes()));
    }
  }
}
