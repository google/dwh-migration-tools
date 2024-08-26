/*
 * Copyright 2022-2024 Google LLC
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
package com.google.edwmigration.dumper.application.dumper;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dumper supports Java 8 or higher. This class provides utilities to determine, whether it is
 * currently running on the supported version of Java.
 */
public class JavaVersionChecker {
  private static final Logger LOG = LoggerFactory.getLogger(JavaVersionChecker.class);

  /**
   * Returns Java major version.
   *
   * @return Java major (or feature) version, or null if the detection failed
   */
  @Nullable
  public static Integer getJavaVersion() {
    Integer javaFeatureOrMajorVersion = getJavaFeatureOrMajorVersion();
    if (javaFeatureOrMajorVersion != null) {
      return javaFeatureOrMajorVersion;
    }
    String version = System.getProperty("java.version");
    LOG.info("Detected Java version: '{}'.", version);
    if (!version.startsWith("1.")) {
      return null;
    }
    Pattern oldVersionPattern = Pattern.compile("^1\\.([0-9]+)");
    Matcher matcher = oldVersionPattern.matcher(version);
    if (matcher.find()) {
      String match = matcher.group(1);
      if (match.length() != 1) {
        LOG.warn("Unrecognized Java version '{}'.", version);
        return null;
      }
      try {
        return Integer.parseInt(match);
      } catch (NumberFormatException e) {
        LOG.warn("Unrecognized Java version '{}'.", version, e);
      }
    }
    return null;
  }

  /**
   * Determines the currently running Java version, if it Java 9 or higher.
   *
   * @return the feature version or the major version of Java, or null for versions lower than 9 or
   *     if the detection failed
   */
  @Nullable
  private static Integer getJavaFeatureOrMajorVersion() {
    Object version;
    try {
      version = call(Runtime.getRuntime(), "version");
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
      // ignoring as the method is not available in the Java version that we are running currently
      return null;
    }
    Object featureVersion;
    try {
      featureVersion = call(version, "feature");
      if (featureVersion != null) {
        return (Integer) featureVersion;
      }
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
      // ignoring as the method is not available in the Java version that we are running currently
    }
    try {
      Object majorVersion = call(version, "major");
      if (majorVersion != null) {
        return (Integer) majorVersion;
      }
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
      // ignoring as the method is not available in the Java version that we are running currently
    }
    return null;
  }

  private static Object call(Object obj, String methodName)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method method = obj.getClass().getDeclaredMethod(methodName);
    method.setAccessible(true);
    return method.invoke(obj);
  }
}
