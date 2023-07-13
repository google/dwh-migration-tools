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

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumper;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger LOG =
      LoggerFactory.getLogger(com.google.edwmigration.dumper.application.dumper.Main.class);

  private static final Gson GSON = new Gson();

  private final Supplier<MetadataDumper> metadataDumperSupplier;
  private final MetadataRetriever metadataRetriever;

  Main(Supplier<MetadataDumper> metadataDumperSupplier, MetadataRetriever metadataRetriever) {
    this.metadataDumperSupplier = metadataDumperSupplier;
    this.metadataRetriever = metadataRetriever;
  }

  void run() throws Exception {
    ExtractorConfiguration config =
        metadataRetriever
            .getAttribute("dwh_extractor_configuration")
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
      args.addAll(connectorConfiguration.args);
      ConnectorArguments arguments = new ConnectorArguments(args.toArray(new String[args.size()]));
      metadataDumperSupplier.get().run(arguments);
    }
  }

  public static void main(String... args) throws Exception {
    try {
      new Main(
              () -> new MetadataDumper(),
              new HttpClientMetadataRetriever(HttpClients.createDefault()))
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
}
