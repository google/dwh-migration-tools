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
import com.google.edwmigration.validation.deformed.Deformed;
import com.google.edwmigration.validation.model.UserInputContext;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.moandjiezana.toml.Toml;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/** Handles configuration file reading and parsing */
public class ConfigLoader {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigLoader.class);

  private static Toml readTomlFile(String path) {
    File file = new File(path);
    if (!file.exists()) {
      throw new IllegalArgumentException("Config file does not exist: " + path);
    }
    return new Toml().read(file);
  }

  private static Map<String, Object> readYamlFile(String path) throws FileNotFoundException {
    File file = new File(path);
    if (!file.exists()) {
      throw new IllegalArgumentException("Config file does not exist: " + path);
    }
    Yaml yaml = new Yaml();
    return yaml.load(new FileReader(file));
  }

  private static UserInputContext deserializeToml(Toml toml) {
    return toml.to(UserInputContext.class);
  }

  private static void fallbackValidation(Map<String, Object> yamlMap, Exception originalError) {
    Gson gson = new Gson();
    JsonObject rawJson = gson.toJsonTree(yamlMap).getAsJsonObject();

    JsonObject scJson = rawJson.getAsJsonObject("sourceConnection");
    if (scJson == null) {
      LOG.error("Invalid or missing `sourceConnection` block in YAML config: ", originalError);
      System.exit(1);
    }

    SourceConnection sc = gson.fromJson(scJson, SourceConnection.class);
    Deformed<SourceConnection> validator = new Deformed<>(SourceConnection.buildSchema());
    validator.validate(sc);

    if (!validator.isValid) {
      LOG.error("Configuration errors detected:", validator.validationState);
    } else {
      LOG.error(
          "Unexpected deserialization error. Please inspect your config file.", originalError);
    }
    System.exit(1);
  }

  private static void fallbackValidation(Toml toml, Exception originalError) {
    Gson gson = new Gson();
    JsonObject rawJson = gson.toJsonTree(toml.toMap()).getAsJsonObject();

    JsonObject scJson = rawJson.getAsJsonObject("SourceConnection");
    if (scJson == null) {
      LOG.error("Invalid or missing `sourceConnection` block in TOML config: ", originalError);
      System.exit(1);
    }

    SourceConnection sc = gson.fromJson(scJson, SourceConnection.class);
    Deformed<SourceConnection> validator = new Deformed<>(SourceConnection.buildSchema());
    validator.validate(sc);

    if (!validator.isValid) {
      LOG.error("Configuration errors detected:", validator.validationState);
    } else {
      LOG.error(
          "Unexpected deserialization error. Please inspect your config file.", originalError);
    }
    System.exit(1);
  }

  public static UserInputContext load(String path) {
    try {
      if (path.endsWith(".yaml") || path.endsWith(".yml")) {
        LOG.info("Detected YAML configuration file.");
        Map<String, Object> yamlMap = readYamlFile(path);
        Gson gson = new Gson();
        JsonObject json = gson.toJsonTree(yamlMap).getAsJsonObject();
        return gson.fromJson(json, UserInputContext.class);
      } else {
        LOG.info("Assuming TOML configuration file.");
        Toml toml = readTomlFile(path);
        return deserializeToml(toml);
      }
    } catch (Exception e) {
      LOG.warn("Failed to load configuration. Inspecting for errors...");

      if (path.endsWith(".yaml") || path.endsWith(".yml")) {
        try {
          fallbackValidation(readYamlFile(path), e);
        } catch (Exception inner) {
          LOG.error("YAML fallback validation failed.");
          LOG.error("Root cause:", inner);
        }
      } else {
        try {
          fallbackValidation(readTomlFile(path), e);
        } catch (Exception inner) {
          LOG.error("TOML fallback validation failed.");
          LOG.error("Root cause:", inner);
        }
      }
      return null;
    }
  }
}
