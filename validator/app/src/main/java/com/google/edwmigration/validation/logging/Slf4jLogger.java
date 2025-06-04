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

import org.slf4j.LoggerFactory;

public class Slf4jLogger implements LoggerWrapper {
  private final org.slf4j.Logger logger;

  public Slf4jLogger() {
    this.logger = LoggerFactory.getLogger("Validation");
  }

  public Slf4jLogger(Class<?> clazz) {
    this.logger = LoggerFactory.getLogger(clazz);
  }

  @Override
  public void info(String message) {
    logger.info(message);
  }

  @Override
  public void error(String message) {
    logger.error(message);
  }

  @Override
  public void error(String message, Throwable t) {
    logger.error(message, t);
  }

  @Override
  public void debug(String message) {
    logger.debug(message);
  }

  @Override
  public void warn(String message) {
    logger.warn(message);
  }
}
