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

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.model.SparkApplicationType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SparkApplicationTypeDetectorTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void detect_sparkR_viaRRunner() {
    ObjectNode rootNode =
        createRootWithProp(
            "System Properties", "sun.java.command", "org.apache.spark.deploy.RRunner");

    SparkApplicationType result = SparkApplicationTypeDetector.detect(rootNode);

    assertEquals(SparkApplicationType.SPARK_R, result);
  }

  @Test
  public void detect_sparkR_viaRCommand() {
    ObjectNode rootNode = createRootWithProp("Spark Properties", "spark.r.command", "Rscript");

    SparkApplicationType result = SparkApplicationTypeDetector.detect(rootNode);

    assertEquals(SparkApplicationType.SPARK_R, result);
  }

  @Test
  public void detect_sparkR_viaPrimaryResource() {
    ObjectNode rootNode =
        createRootWithProp("Spark Properties", "spark.yarn.primary.py.file", "script.R");

    SparkApplicationType result = SparkApplicationTypeDetector.detect(rootNode);

    assertEquals(SparkApplicationType.SPARK_R, result);
  }

  @Test
  public void detect_sparkSql_viaSqlCliDriver() {
    ObjectNode rootNode =
        createRootWithProp(
            "System Properties",
            "sun.java.command",
            "org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver");

    SparkApplicationType result = SparkApplicationTypeDetector.detect(rootNode);

    assertEquals(SparkApplicationType.SPARK_SQL, result);
  }

  @Test
  public void detect_sparkSql_viaThriftServer() {
    ObjectNode rootNode =
        createRootWithProp(
            "System Properties",
            "sun.java.command",
            "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2");

    SparkApplicationType result = SparkApplicationTypeDetector.detect(rootNode);

    assertEquals(SparkApplicationType.SPARK_SQL, result);
  }

  @Test
  public void detect_sparkSql_viaAppName() {
    ObjectNode rootNode = createRootWithProp("Spark Properties", "spark.app.name", "SparkSQL");

    SparkApplicationType result = SparkApplicationTypeDetector.detect(rootNode);

    assertEquals(SparkApplicationType.SPARK_SQL, result);
  }

  @Test
  public void detect_pig() {
    ObjectNode rootNode =
        createRootWithProp("System Properties", "sun.java.command", "org.apache.pig.Main");

    SparkApplicationType result = SparkApplicationTypeDetector.detect(rootNode);

    assertEquals(SparkApplicationType.PIG, result);
  }

  @Test
  public void detect_pySpark_viaPythonPath() {
    ObjectNode rootNode =
        createRootWithProp(
            "Spark Properties", "spark.executorEnv.PYTHONPATH", "/some/path/pyspark.zip");

    SparkApplicationType result = SparkApplicationTypeDetector.detect(rootNode);

    assertEquals(SparkApplicationType.PYSPARK, result);
  }

  @Test
  public void detect_pySpark_viaPrimaryResource() {
    ObjectNode rootNode =
        createRootWithProp("Spark Properties", "spark.yarn.primary.py.file", "script.py");

    SparkApplicationType result = SparkApplicationTypeDetector.detect(rootNode);

    assertEquals(SparkApplicationType.PYSPARK, result);
  }

  @Test
  public void detect_scalaJava_viaJarInCommand() {
    ObjectNode rootNode = createRootWithProp("System Properties", "sun.java.command", "my-app.jar");

    SparkApplicationType result = SparkApplicationTypeDetector.detect(rootNode);

    assertEquals(SparkApplicationType.SCALA_JAVA, result);
  }

  @Test
  public void detect_scalaJava_viaSparkJarsProp() {
    ObjectNode rootNode = createRootWithProp("Spark Properties", "spark.jars", "file:/path/to.jar");

    SparkApplicationType result = SparkApplicationTypeDetector.detect(rootNode);

    assertEquals(SparkApplicationType.SCALA_JAVA, result);
  }

  @Test
  public void detect_empty_whenNoMatch() {
    ObjectNode rootNode = createRootWithProp("Spark Properties", "spark.app.name", "GenericApp");

    SparkApplicationType result = SparkApplicationTypeDetector.detect(rootNode);

    assertEquals(SparkApplicationType.UNKNOWN, result);
  }

  @Test
  public void detect_empty_whenNoProperties() {
    ObjectNode rootNode = mapper.createObjectNode();

    SparkApplicationType result = SparkApplicationTypeDetector.detect(rootNode);

    assertEquals(SparkApplicationType.UNKNOWN, result);
  }

  private ObjectNode createRootWithProp(String root, String key, String value) {
    ObjectNode rootNode = mapper.createObjectNode();
    ObjectNode props = rootNode.putObject(root);
    props.put(key, value);
    return rootNode;
  }
}
