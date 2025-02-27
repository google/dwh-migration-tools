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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.io.FileUtils;

public class ScriptTmpDirCleanup {

  private static final AtomicBoolean hookRegistered = new AtomicBoolean();

  public static void cleanupAfterAllTestsAreFinished() {
    if (hookRegistered.compareAndSet(false, true)) {
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    try {
                      FileUtils.deleteDirectory(HadoopScripts.getScriptDir().toFile());
                    } catch (IOException ex) {
                      ex.printStackTrace();
                    }
                  }));
    }
  }
}
