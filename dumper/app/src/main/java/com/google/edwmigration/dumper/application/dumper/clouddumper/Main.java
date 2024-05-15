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
package com.google.edwmigration.dumper.application.dumper.clouddumper;

import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.edwmigration.dumper.application.dumper.MetadataDumper;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.commons.codec.binary.Base64;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.util.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger LOG =
      LoggerFactory.getLogger(com.google.edwmigration.dumper.application.dumper.Main.class);

  private static final Gson GSON = new Gson();

  private final Supplier<MetadataDumper> metadataDumperSupplier;
  private final MetadataRetriever metadataRetriever;
  private final DriverRetriever driverRetriever;

  Main(
      Supplier<MetadataDumper> metadataDumperSupplier,
      MetadataRetriever metadataRetriever,
      DriverRetriever driverRetriever) {
    this.metadataDumperSupplier = metadataDumperSupplier;
    this.metadataRetriever = metadataRetriever;
    this.driverRetriever = driverRetriever;
  }

  void run() throws Exception {
    Optional<CryptoKeyName> keyName =
        metadataRetriever.getAttribute("dwh_key").map(CryptoKeyName::parse);
    ExtractorConfiguration config =
        metadataRetriever
            .getAttribute("dwh_extractor_configuration")
            .map(s -> decrypt(keyName, s))
            .map(s -> GSON.fromJson(s, ExtractorConfiguration.class))
            .orElseThrow(
                () ->
                    new MetadataDumperUsageException(
                        ("Attribute dwh_extractor_configuration is not defined.")));
    if (config.connectors == null || config.connectors.isEmpty()) {
      throw new MetadataDumperUsageException(
          "Extractor configuration must provide at least one connector.");
    }
    for (ConnectorConfiguration connectorConfiguration : config.connectors) {
      ArrayList<String> args = new ArrayList<>();
      args.add("--connector");
      args.add(connectorConfiguration.connector);
      driverRetriever
          .getDriver(connectorConfiguration.connector)
          .ifPresent(
              driverPath -> {
                args.add("--driver");
                args.add(driverPath.toString());
              });
      args.addAll(connectorConfiguration.args);
      metadataDumperSupplier.get().run(args.toArray(new String[args.size()]));
    }
  }

  public static void main(String... args) throws Exception {
    try (CloseableHttpClient httpClient =
        HttpClientBuilder.create()
            .setRetryStrategy(
                new DefaultHttpRequestRetryStrategy(
                    /* maxRetries= */ 3, /* defaultRetryInterval= */ TimeValue.ofSeconds(1L)))
            .build()) {
      new Main(
              () -> new MetadataDumper(),
              new HttpClientMetadataRetriever(httpClient),
              DriverRetriever.create(httpClient, Files.createTempDirectory("clouddumper")))
          .run();
    } catch (MetadataDumperUsageException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  static class ExtractorConfiguration {

    private List<ConnectorConfiguration> connectors;
  }

  static class ConnectorConfiguration {

    private String connector;

    private List<String> args;
  }

  private static final String decrypt(Optional<CryptoKeyName> keyName, String input) {
    if (!keyName.isPresent()) {
      LOG.info("Got no decryption key. Using input as is.");
      return input;
    }
    LOG.info("Using key {} to decrypt.", keyName);
    ByteString ciphertext = ByteString.copyFrom(Base64.decodeBase64(input));
    try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {
      DecryptResponse response = client.decrypt(keyName.get(), ciphertext);
      return response.getPlaintext().toString(StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
