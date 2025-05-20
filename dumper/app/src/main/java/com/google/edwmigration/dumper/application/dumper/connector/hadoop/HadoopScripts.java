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
package com.google.edwmigration.dumper.application.dumper.connector.hadoop;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.thirdparty.com.google.common.collect.Maps.immutableEntry;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopScripts {
  private static final Logger logger = LoggerFactory.getLogger(HadoopScripts.class);
  private static final String TMP_DIR_PREFIX = "dwh-migration-tools-tmp-";
  private static final Supplier<Path> SCRIPT_DIR_SUPPLIER =
      memoize(
          () -> {
            Path tmpDir;
            Path workingDir = Paths.get("");
            logger.info(
                "Creating temporary directory in the working directory '{}'...",
                workingDir.toAbsolutePath());
            try {
              tmpDir = Files.createTempDirectory(workingDir, TMP_DIR_PREFIX);
            } catch (IOException e) {
              throw new IllegalStateException("Error creating temporary directory.", e);
            }
            logger.info("Temporary directory created: '{}'.", tmpDir);
            return tmpDir;
          });

  /** Reads the script from the resources directory inside the jar. */
  public static byte[] read(String scriptFilename) throws IOException {
    URL resourceUrl = Resources.getResource("hadoop-scripts/" + scriptFilename);
    return Resources.toByteArray(resourceUrl);
  }

  /** Extracts the script from the resources directory inside the jar to the local filesystem. */
  public static Path extract(String scriptFilename) {
    try {
      return create(scriptFilename, HadoopScripts.read(scriptFilename));
    } catch (IOException e) {
      throw new IllegalStateException(
          String.format("Cannot create the bash task for the script '%s'.", scriptFilename), e);
    }
  }

  public static ImmutableMap<String, Path> extractSingleLineScripts() {
    ImmutableMap<String, String> singleLineScripts = readSingleLineScripts();
    return singleLineScripts.entrySet().stream()
        .map(
            entry -> {
              String scriptName = entry.getKey();
              String scriptBody = entry.getValue();
              try {
                return immutableEntry(
                    scriptName,
                    HadoopScripts.create(scriptName + ".sh", scriptBody.getBytes(UTF_8)));
              } catch (IOException e) {
                throw new IllegalStateException(
                    String.format("Cannot create the bash task for the script '%s'.", scriptName),
                    e);
              }
            })
        .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static ImmutableMap<String, String> readSingleLineScripts() {
    ObjectMapper mapper = new ObjectMapper();
    String filename = "single-line-scripts.json";
    Map<String, String> scriptMap;
    try {
      byte[] singleLineScriptsJson = HadoopScripts.read(filename);
      scriptMap =
          mapper.readValue(singleLineScriptsJson, new TypeReference<HashMap<String, String>>() {});
    } catch (IOException e) {
      throw new IllegalStateException(String.format("Cannot read '%s'", filename), e);
    }
    return scriptMap.entrySet().stream()
        .map(valueMapper(script -> "#!/bin/bash\n\n" + script + "\n"))
        .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static Function<Entry<String, String>, Entry<String, String>> valueMapper(
      Function<String, String> mapper) {
    return entry -> immutableEntry(entry.getKey(), mapper.apply(entry.getValue()));
  }

  public static Path create(String scriptFilename, byte[] scriptBody) throws IOException {
    Path scriptDir = SCRIPT_DIR_SUPPLIER.get();
    checkState(
        Files.exists(scriptDir) && Files.isDirectory(scriptDir),
        "Directory '%s' does not exist.",
        scriptDir);
    logger.info("Extracting '{}' to '{}'...", scriptFilename, scriptDir);
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
