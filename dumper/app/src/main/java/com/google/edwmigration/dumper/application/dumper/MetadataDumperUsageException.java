/*
 * Copyright 2022-2023 Google LLC
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

import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * For exceptions that should halt the dumper, but for which we do not need to show the user a stack
 * trace (i.e., invalid arguments to the dumper). Do not use this for JDBC/filesystem/etc issues; if
 * a customer experiences any of those, we want to know all of the gory details.
 *
 * @author matt
 */
public class MetadataDumperUsageException extends Exception {

  private final List<String> msgs;

  @SuppressWarnings("unchecked")
  public MetadataDumperUsageException(@Nonnull String msg) {
    this(msg, Collections.emptyList());
  }

  public MetadataDumperUsageException(@Nonnull String msg, @Nonnull List<String> msgs) {
    super(msg);
    this.msgs = msgs;
  }

  public List<String> getMessages() {
    return msgs;
  }

  @Override
  public String toString() {
    return this.getMessage() + "\n" + String.join("\n", msgs);
  }
}
