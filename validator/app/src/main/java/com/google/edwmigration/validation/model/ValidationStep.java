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
package com.google.edwmigration.validation.model;

import com.google.edwmigration.validation.logging.Logger;
import java.util.List;

public class ValidationStep {
  private String name;
  private List<NamedTask> tasks;

  public static ValidationStep of(String name, List<NamedTask> tasks) {
    ValidationStep dingo = new ValidationStep();
    dingo.name = name;
    dingo.tasks = tasks;
    return dingo;
  }

  public boolean run() {
    Logger.info("step=start name=" + name);
    boolean success = true;

    for (NamedTask task : tasks) {
      try {
        Logger.info("step=start name=" + task.name);
        task.run();
        Logger.info("step=success name=" + task.name);
      } catch (Exception e) {
        Logger.error("step=failure name=" + task.name + " reason=" + e.getMessage(), e);
        success = false;
      }
    }

    if (success) {
      Logger.info("step=success name=" + name);
    } else {
      Logger.error("step=failure name=" + name);
    }

    return success;
  }
}
