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
package com.google.edwmigration.dumper.application.dumper.connector.hadoop;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HadoopScripts {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopScripts.class);
  private static final String TMP_DIR_PREFIX = "dwh-migration-tools-tmp-";
  private static final Supplier<Path> SCRIPT_DIR_SUPPLIER =
      memoize(
          () -> {
            Path tmpDir;
            Path workingDir = Paths.get("");
            LOG.info(
                "Creating temporary directory in the working directory '{}'...",
                workingDir.toAbsolutePath());
            try {
              tmpDir = Files.createTempDirectory(workingDir, TMP_DIR_PREFIX);
            } catch (IOException e) {
              throw new IllegalStateException("Error creating temporary directory.", e);
            }
            LOG.info("Temporary directory created: '{}'.", tmpDir);
            return tmpDir;
          });

  private static final Supplier<ImmutableMap<String, String>> SINGLE_LINE_SCRIPTS =
      memoize(
          () -> {
            String filename = "single-line-scripts.txt";
            try {
              return Splitter.on('\n')
                  .trimResults()
                  .omitEmptyStrings()
                  .splitToStream(new String(read(filename)))
                  .flatMap(
                      line -> {
                        int firstColon = line.indexOf(':');
                        if (firstColon > 0) {
                          return Stream.of(
                              Pair.of(
                                  line.substring(0, firstColon).trim(),
                                  "#!/bin/bash\n\n"
                                      + line.substring(firstColon + 1).trim()
                                      + "\n"));
                        } else {
                          return Stream.empty();
                        }
                      })
                  .collect(toImmutableMap(Pair::getLeft, Pair::getRight));
            } catch (IOException e) {
              throw new IllegalStateException(String.format("Cannot read '%s'", filename), e);
            }
          });

  /** Reads the script from the resources directory inside the jar. */
  static byte[] read(String scriptFilename) throws IOException {
    URL resourceUrl = Resources.getResource("hadoop-scripts/" + scriptFilename);
    return Resources.toByteArray(resourceUrl);
  }

  /** Extracts the script from the resources directory inside the jar to the local filesystem. */
  static Path extract(String scriptFilename) throws IOException {
    return extract(scriptFilename, HadoopScripts.read(scriptFilename));
  }

  static Path extractSingleLineScript(String scriptName) throws IOException {
    return extract(scriptName + ".sh", SINGLE_LINE_SCRIPTS.get().get(scriptName).getBytes(UTF_8));
  }

  private static Path extract(String scriptFilename, byte[] scriptBody) throws IOException {
    Path scriptDir = SCRIPT_DIR_SUPPLIER.get();
    checkState(
        Files.exists(scriptDir) && Files.isDirectory(scriptDir),
        "Directory '%s' does not exist.",
        scriptDir);
    LOG.info("Extracting '{}' to '{}'...", scriptFilename, scriptDir);
    Path scriptPath = scriptDir.resolve(scriptFilename);
    Files.write(scriptPath, scriptBody);
    scriptPath.toFile().setExecutable(true);
    return scriptPath;
  }

  @VisibleForTesting
  static Path getScriptDir() {
    return SCRIPT_DIR_SUPPLIER.get();
  }
}
