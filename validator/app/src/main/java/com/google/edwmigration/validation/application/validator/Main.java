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
package com.google.edwmigration.validation.application.validator;

import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author nehanene */
public class Main {

  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  private final Validator validator;

  public Main(Validator validator) {
    this.validator = validator;
  }

  public boolean run(@Nonnull String... args) throws Exception {
    return validator.run(args);
  }

  public static void main(String... args) {
    try {
      Main main = new Main(new Validator());
      if (args.length == 0) {
        args = new String[] {"--help"};
      }
      if (!main.run(args)) {
        System.exit(1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
