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

import com.google.edwmigration.validation.core.Validator;
import com.google.edwmigration.validation.io.ConfigLoader;
import com.google.edwmigration.validation.model.UserInputContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    if (args.length < 1) {
      LOG.error("Usage: java Main <path-to-config.toml>");
      System.exit(1);
    }

    String configPath = args[0];
    UserInputContext config = ConfigLoader.load(configPath);
    if (config == null) {
      LOG.error("Failed to load config");
      System.exit(1);
    }

    Validator validator = new Validator(config);
    boolean success = validator.run();

    System.exit(success ? 0 : 1);
  }
}
