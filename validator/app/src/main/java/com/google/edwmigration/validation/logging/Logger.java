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
package com.google.edwmigration.validation.logging;

public class Logger {
  private static LoggerWrapper logger = new ConsoleLogger(); // default

  public static void setLogger(LoggerWrapper customLogger) {
    logger = customLogger;
  }

  public static void info(String message) {
    logger.info(message);
  }

  public static void error(String message) {
    logger.error(message);
  }

  public static void error(String message, Throwable t) {
    logger.error(message, t);
  }

  public static void debug(String message) {
    logger.debug(message);
  }

  public static void warn(String message) {
    logger.warn(message);
  }
}
