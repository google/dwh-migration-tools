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
package com.google.edwmigration.validation.deformed;

import java.util.function.Function;

/** Represents a single validation rule */
public class ValidationProperty<S> {
  public final Function<S, String> errorMessage;
  public final Predicate<S> predicate;

  public ValidationProperty(Function<S, String> errorMessage, Predicate<S> predicate) {
    this.errorMessage = errorMessage;
    this.predicate = predicate;
  }

  public static <S> ValidationProperty<S> of(String staticMessage, Predicate<S> predicate) {
    return new ValidationProperty<>(s -> staticMessage, predicate);
  }

  public static <S> ValidationProperty<S> of(
      Function<S, String> dynamicMessage, Predicate<S> predicate) {
    return new ValidationProperty<>(dynamicMessage, predicate);
  }

  public static <S> ValidationProperty<S> requiredString(
      String error, Function<S, String> selector) {
    return new ValidationProperty<>(
        s -> error,
        s -> {
          String value = selector.apply(s);
          return value != null && !value.trim().isEmpty();
        });
  }
}
