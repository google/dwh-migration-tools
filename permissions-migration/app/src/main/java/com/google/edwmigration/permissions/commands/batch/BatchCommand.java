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
package com.google.edwmigration.permissions.commands.batch;

import com.google.edwmigration.permissions.GcsPath;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchCommand {

  private static final Logger LOG = LoggerFactory.getLogger(BatchCommand.class);

  public void run(String args[]) throws IOException {
    BatchOptions opts = new BatchOptions(args);
    if (opts.handleHelp()) {
      return;
    }

    Batcher batcher =
        new Batcher(
            GcsPath.parse(opts.getSourcePath()),
            GcsPath.parse(opts.getTargetPath()),
            opts.getPattern(),
            opts.getNumThreads(),
            opts.getTimeoutSeconds());
    batcher.Run();
  }
}
