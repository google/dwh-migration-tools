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
package com.google.edwmigration.validation;

import com.google.edwmigration.validation.config.ValidationConfig;
import com.google.edwmigration.validation.core.Validator;
import com.google.edwmigration.validation.io.ConfigLoader;
import com.google.edwmigration.validation.logging.ConsoleLogger;
import com.google.edwmigration.validation.logging.Logger;
import com.google.edwmigration.validation.logging.Slf4jLogger;

public class Main {
  public static void main(String[] args) {
    String loggerType = System.getenv("VALIDATOR_LOGGER");
    if ("slf4j".equalsIgnoreCase(loggerType)) {
      Logger.setLogger(new Slf4jLogger(Main.class));
    } else {
      Logger.setLogger(new ConsoleLogger());
    }

    if (args.length < 1) {
      Logger.error("Usage: java Main <path-to-config.toml>");
      System.exit(1);
    }

    String configPath = args[0];
    ValidationConfig config = ConfigLoader.load(configPath);
    if (config == null) {
      Logger.error("❌ Failed to load config — is your TOML file malformed?");
      System.exit(1);
    }

    Validator validator = new Validator(config);
    boolean success = validator.run();

    System.exit(success ? 0 : 1);
  }
}
