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
package com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model;

import com.google.auto.value.AutoValue;
import java.util.Optional;

@AutoValue
public abstract class Projection {
  public abstract Expression expression();

  public abstract Optional<String> alias();

  public static Projection create(Expression expression, Optional<String> alias) {
    return new AutoValue_Projection(expression, alias);
  }
}
