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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager;

import static com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.model.SparkApplicationType.PIG;
import static com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.model.SparkApplicationType.PYSPARK;
import static com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.model.SparkApplicationType.SCALA_JAVA;
import static com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.model.SparkApplicationType.SPARK_R;
import static com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.model.SparkApplicationType.SPARK_SQL;
import static com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.model.SparkApplicationType.UNKNOWN;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.model.SparkApplicationType;

public class SparkApplicationTypeDetector {

  private SparkApplicationTypeDetector() {}

  public static SparkApplicationType detect(JsonNode rootNode) {
    JsonNode sparkProps = rootNode.get("Spark Properties");
    if (sparkProps == null) return UNKNOWN;

    String javaCmd = getProp(sparkProps, "sun.java.command");
    String appName = getProp(sparkProps, "spark.app.name");
    String primaryResource = getProp(sparkProps, "spark.yarn.primary.py.file");
    String pythonPath = getProp(sparkProps, "spark.executorEnv.PYTHONPATH");
    String rCommand = getProp(sparkProps, "spark.r.command");

    if (javaCmd.contains("org.apache.spark.deploy.RRunner")
        || !rCommand.isEmpty()
        || primaryResource.toLowerCase().endsWith(".r")) {
      return SPARK_R;
    }

    if (javaCmd.contains("SparkSQLCLIDriver")
        || javaCmd.contains("HiveThriftServer2")
        || "SparkSQL".equalsIgnoreCase(appName)) {
      return SPARK_SQL;
    }

    if (javaCmd.contains("org.apache.pig.Main")) {
      return PIG;
    }

    if (pythonPath.contains("pyspark") || primaryResource.endsWith(".py")) {
      return PYSPARK;
    }

    if (javaCmd.contains(".jar") || sparkProps.has("spark.jars")) {
      return SCALA_JAVA;
    }

    return UNKNOWN;
  }

  private static String getProp(JsonNode props, String key) {
    return props.has(key) ? props.get(key).asText() : "";
  }
}
