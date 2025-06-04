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

import java.util.Map;
import java.util.function.Function;

/**
 * WIP - not quite working yet... problem for a later date the idea is to make it simple, or even
 * automatic, to pull up nested errors from nested data objects without too much boiler plate
 */
public class NestedValidationProperty<S> extends ValidationProperty<S> {

  public final Function<S, Map.Entry<Boolean, ValidationState>> nestedValidator;

  public NestedValidationProperty(
      Function<S, String> errorMessage,
      Function<S, Map.Entry<Boolean, ValidationState>> nestedValidator) {
    super(errorMessage, s -> nestedValidator.apply(s).getKey());
    this.nestedValidator = nestedValidator;
  }

  public ValidationState getNestedState(S state) {
    return nestedValidator.apply(state).getValue();
  }
}
