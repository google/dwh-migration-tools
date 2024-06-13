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

import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HadoopScripts {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopScripts.class);
  private static final String TMP_DIR_PREFIX = "dwh-migration-tools-tmp-";
  private static final Supplier<Path> SCRIPT_DIR_SUPPLIER =
      memoize(
          () -> {
            Path tmpDir;
            LOG.info("Creating temporary directory...");
            try {
              tmpDir = Files.createTempDirectory(TMP_DIR_PREFIX);
            } catch (IOException e) {
              LOG.error("Error creating temporary directory in the system temporary directory.", e);
              Path workingDir = Paths.get("");
              LOG.info(
                  "Falling back to creating the temporary directory in the working directory: '{}'.",
                  workingDir.toAbsolutePath());
              try {
                tmpDir = Files.createTempDirectory(workingDir, TMP_DIR_PREFIX);
              } catch (IOException e2) {
                throw new IllegalStateException("Error creating temporary directory.", e2);
              }
            }
            LOG.info("Temporary directory created: '{}'.", tmpDir);
            return tmpDir;
          });

  /** Reads the script from the resources directory inside the jar. */
  static byte[] read(String scriptFilename) throws IOException {
    URL resourceUrl = Resources.getResource("hadoop-scripts/" + scriptFilename);
    return Resources.toByteArray(resourceUrl);
  }

  /** Extracts the script from the resources directory inside the jar to the local filesystem. */
  static synchronized Path extract(String scriptFilename) throws IOException {
    byte[] scriptBody = HadoopScripts.read(scriptFilename);
    Path scriptDir = SCRIPT_DIR_SUPPLIER.get();
    checkState(
        Files.exists(scriptDir) && Files.isDirectory(scriptDir),
        "Temporary directory '%s' does not exist.",
        scriptDir);
    Path scriptPath = scriptDir.resolve(scriptFilename);
    Files.write(scriptPath, scriptBody);
    scriptPath.toFile().setExecutable(true);
    return scriptPath;
  }
}
