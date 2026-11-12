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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Extracts a script, records the script directory in the file given as the first argument and
 * exits. Started in a separate JVM to observe what is left behind after the JVM exits.
 */
public class HadoopScriptsExitProbe {

  public static void main(String[] args) throws IOException {
    Path script = HadoopScripts.extract("hadoop-version.sh");
    Files.write(Paths.get(args[0]), script.getParent().toAbsolutePath().toString().getBytes(UTF_8));
  }
}
