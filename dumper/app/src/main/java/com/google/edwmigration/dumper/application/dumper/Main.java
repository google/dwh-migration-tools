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
package com.google.edwmigration.dumper.application.dumper;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import one.profiler.AsyncProfiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author miguel */
public class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  private static void printErrorMessages(Throwable e) {
    new SummaryPrinter()
        .printSummarySection(
            linePrinter -> {
              linePrinter.println("ERROR");
              ImmutableList<String> errorMessages =
                  Throwables.getCausalChain(e).stream()
                      .map(Throwable::getMessage)
                      .filter(Objects::nonNull)
                      .collect(toImmutableList());
              for (int i = 0; i < errorMessages.size(); i++) {
                String errorMessage = errorMessages.get(i);
                if (i > 0) {
                  errorMessage = "Caused by: " + errorMessage;
                }
                linePrinter.println(errorMessage);
              }
            });
  }

  public static void main(String... args) throws Exception {
    try {
      AsyncProfiler asyncProfiler = AsyncProfiler.getInstance();
      asyncProfiler.execute("start,event=cpu,interval=10ms");

      StartUpMetaInfoProcessor.printMetaInfo();

      if (args.length == 0) {
        args = new String[] {"--help"};
      }

      MetadataDumper metadataDumper =
          new MetadataDumper(
              (outputFileLocation) -> {
                stopProfileAndAppendToZip(asyncProfiler, outputFileLocation);
              },
              args);

      if (!metadataDumper.run()) {
        System.exit(1);
      }
    } catch (MetadataDumperUsageException e) {
      logger.error(e.getMessage());
      for (String msg : e.getMessages()) {
        logger.error(msg);
      }
      System.exit(1);
    } catch (Exception e) {
      e.printStackTrace();
      printErrorMessages(e);
      System.exit(1);
    }
  }

  private static void stopProfileAndAppendToZip(
      AsyncProfiler asyncProfiler, String outputFileLocation) {
    try {
      File tempFlameGraph = File.createTempFile("flamegraph", ".html");
      String stopCommand = "stop,output=flamegraph,file=" + tempFlameGraph.getAbsolutePath();
      asyncProfiler.execute(stopCommand);

      moveFileToZip(outputFileLocation, tempFlameGraph, "flamegraph.html");
    } catch (Exception ignored) {
    }
  }

  private static void moveFileToZip(String zipFile, File file, String entryName)
      throws IOException {
    Map<String, String> env = Collections.singletonMap("create", "false");
    URI zipUri = URI.create("jar:" + Paths.get(zipFile).toUri());

    try (FileSystem zipFs = FileSystems.newFileSystem(zipUri, env)) {
      Path pathInZip = zipFs.getPath(entryName);

      Files.copy(file.toPath(), pathInZip, StandardCopyOption.REPLACE_EXISTING);
      Files.deleteIfExists(file.toPath());
    }
  }
}
