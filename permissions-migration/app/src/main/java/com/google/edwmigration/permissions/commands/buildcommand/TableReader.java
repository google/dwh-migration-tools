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
package com.google.edwmigration.permissions.commands.buildcommand;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.permissions.GcsParallelObjectsProcessor;
import com.google.edwmigration.permissions.GcsPath;
import com.google.edwmigration.permissions.commands.expand.StreamProcessor;
import com.google.edwmigration.permissions.models.Table;
import com.google.edwmigration.permissions.models.TableIdParser;
import com.google.edwmigration.permissions.models.TableTranslationService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableReader implements StreamProcessor<Table> {

  private static final Logger LOG = LoggerFactory.getLogger(TableReader.class);

  private static final String YAML_SUFFIX = ".yaml";

  private final int numThreads;
  private final int timeoutSeconds;
  private final ImmutableList<Table> tables;

  public TableReader(String tablesYaml, int numThreads, int timeoutSeconds) {
    this.numThreads = numThreads;
    this.timeoutSeconds = timeoutSeconds;
    tables = readTables(tablesYaml);
  }

  @Override
  public <R> R process(Function<Stream<Table>, R> operator) {
    return operator.apply(tables.stream());
  }

  private ImmutableList<Table> readTables(String tablesPath) {
    GcsParallelObjectsProcessor gcsParallelObjectsProcessor =
        new GcsParallelObjectsProcessor(GcsPath.parse(tablesPath), numThreads, timeoutSeconds);
    ConcurrentLinkedQueue<Table> tablesQueue = new ConcurrentLinkedQueue<>();
    gcsParallelObjectsProcessor.Run(
        blob -> {
          try {
            if (!blob.getName().toLowerCase().endsWith(YAML_SUFFIX)) {
              return;
            }
            TableTranslationService tableTS =
                TableTranslationService.YAML_MAPPER.readValue(
                    blob.getContent(), TableTranslationService.class);
            TableId bqTableId = TableIdParser.parseTranslationId(tableTS.targetName());
            Table table =
                Table.create(
                    /* name= */ bqTableId.getTable(),
                    /* schemaName= */ bqTableId.getDataset(),
                    /* hdfsPath= */ tableTS.sourceLocations().get(0),
                    /* gcsPath= */ tableTS.targetLocations().get(0),
                    /* bqPath= */ tableTS.targetName());
            tablesQueue.add(table);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Exception while processing blob: %s", blob.getName()), e);
          }
        });
    return tablesQueue.stream().collect(toImmutableList());
  }
}
