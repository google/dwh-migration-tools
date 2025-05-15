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
package com.google.edwmigration.permissions;

import com.google.edwmigration.permissions.commands.apply.ApplyCommand;
import com.google.edwmigration.permissions.commands.batch.BatchCommand;
import com.google.edwmigration.permissions.commands.buildcommand.BuildCommand;
import com.google.edwmigration.permissions.commands.expand.ExpandCommand;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class Main {

  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    if (args.length == 0) {
      usage();
      return;
    }
    try {
      String[] remainingArgs = Arrays.copyOfRange(args, 1, args.length);
      switch (args[0]) {
        case "expand":
          new ExpandCommand().run(remainingArgs);
          break;
        case "build":
          new BuildCommand().run(remainingArgs);
          break;
        case "apply":
          new ApplyCommand().run(remainingArgs);
          break;
        case "batch":
          new BatchCommand().run(remainingArgs);
          break;
        default:
          throw new IllegalArgumentException("Unknown command " + args[0]);
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
    }
  }

  private static void usage() {
    System.err.println("Usage: permissions-migration expand|build|apply|batch [flags]");
    System.err.println(
        "To get help on command use: permissions-migration expand|build|apply|batch --help");
  }
}
