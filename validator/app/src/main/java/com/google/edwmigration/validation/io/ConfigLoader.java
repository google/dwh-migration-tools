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
package com.google.edwmigration.validation.io;

import com.google.edwmigration.validation.config.SourceConnection;
import com.google.edwmigration.validation.config.ValidationConfig;
import com.google.edwmigration.validation.deformed.Deformed;
import com.google.edwmigration.validation.deformed.ErrorFormatter;
import com.google.gson.*;
import com.moandjiezana.toml.Toml;
import java.io.File;

/** Handles TOML configuration file reading and parsing */
public class ConfigLoader {

  private static Toml readTomlFile(String path) {
    File file = new File(path);
    if (!file.exists()) {
      throw new IllegalArgumentException("❌ Config file does not exist: " + path);
    }
    return new Toml().read(file);
  }

  private static ValidationConfig deserializeToml(Toml toml) {
    return toml.to(ValidationConfig.class);
  }

  private static void fallbackValidation(Toml toml, Exception originalError) {
    Gson gson = new Gson();
    JsonObject rawJson = gson.toJsonTree(toml.toMap()).getAsJsonObject();

    JsonObject scJson = rawJson.getAsJsonObject("SourceConnection");
    if (scJson == null) {
      System.err.println("❌ Invalid or missing [SourceConnection] block in config.");
      originalError.printStackTrace();
      System.exit(1);
    }

    SourceConnection sc = gson.fromJson(scJson, SourceConnection.class);
    Deformed<SourceConnection> validator = new Deformed<>(SourceConnection.buildSchema());
    validator.validate(sc);

    if (!validator.isValid) {
      System.err.println("❌ Configuration errors detected:");
      System.err.println(ErrorFormatter.format(validator.validationState));
    } else {
      System.err.println("❌ Unexpected deserialization error. Please inspect your config file.");
      originalError.printStackTrace();
    }
    System.exit(1);
  }

  public static ValidationConfig load(String path) {
    try {
      Toml toml = readTomlFile(path);
      return deserializeToml(toml);
    } catch (Exception e) {
      System.err.println("⚠️ Failed to load configuration. Inspecting for errors...");
      fallbackValidation(readTomlFile(path), e);
      return null; // unreachable
    }
  }
}
