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
package com.google.edwmigration.permissions.commands.buildcommand;

import com.google.edwmigration.permissions.commands.expand.StreamProcessor;
import com.google.edwmigration.permissions.files.FileProcessor;
import com.google.edwmigration.permissions.models.Principal;
import com.google.edwmigration.permissions.models.Principals;
import java.nio.file.Files;
import java.util.function.Function;
import java.util.stream.Stream;

public class PrincipalReader implements StreamProcessor<Principal> {

  private final Principals principals;

  public PrincipalReader(String principalsYaml) {
    principals =
        FileProcessor.apply(
            principalsYaml,
            path -> Principals.YAML_MAPPER.readValue(Files.readAllBytes(path), Principals.class));
  }

  @Override
  public <R> R process(Function<Stream<Principal>, R> operator) {
    return operator.apply(principals.principals().stream());
  }
}
