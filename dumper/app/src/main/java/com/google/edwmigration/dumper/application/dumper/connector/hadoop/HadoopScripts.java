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

import static com.google.common.base.Suppliers.memoize;

import com.google.common.io.Files;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.function.Supplier;

class HadoopScripts {
  private static final Supplier<File> SCRIPT_DIR_SUPPLIER = memoize(Files::createTempDir);

  /** Reads the script from the resources directory inside the jar. */
  static byte[] read(String scriptFilename) throws IOException {
    URL resourceUrl = Resources.getResource("hadoop-scripts/" + scriptFilename);
    return Resources.toByteArray(resourceUrl);
  }

  /** Extracts the script from the resources directory inside the jar to the local filesystem. */
  static synchronized File extract(String scriptFilename) throws IOException {
    byte[] scriptBody = HadoopScripts.read(scriptFilename);
    File scriptFile = new File(SCRIPT_DIR_SUPPLIER.get(), scriptFilename);
    Files.write(scriptBody, scriptFile);
    scriptFile.setExecutable(true);
    return scriptFile;
  }
}
