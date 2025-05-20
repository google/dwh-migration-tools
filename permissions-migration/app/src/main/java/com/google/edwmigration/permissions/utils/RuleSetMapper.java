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
package com.google.edwmigration.permissions.utils;

import com.google.auto.value.AutoValue;

public interface RuleSetMapper<R> extends Mapper<RuleSetMapper.Result<R>> {

  enum Action {
    NO_MATCH,
    MAP,
    SKIP
  }

  @AutoValue
  abstract class Result<T> {

    public static <T> Result<T> create(Action action, T value) {
      return new AutoValue_RuleSetMapper_Result(action, value);
    }

    public abstract Action action();

    public abstract T value();
  }
}
