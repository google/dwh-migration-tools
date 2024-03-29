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
package com.google.edwmigration.dumper.application.dumper.io;

import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.io.Console;
import javax.annotation.Nonnull;

/**
 * Provides the functionality of reading a password from prompt.
 *
 * <p>Needed, because users may not want to provide the password on the command line. This might be
 * for security reasons or due to issues caused by special characters.
 */
public class PasswordReader {

  private boolean hasCachedValue = false;
  @Nonnull private String cachedValue = "";

  @Nonnull
  public synchronized String getOrPrompt() {
    if (hasCachedValue) {
      return cachedValue;
    }
    Console console = System.console();
    if (console == null) {
      // user requested a prompt, but we can't even get a console, so let's just fail
      throw new MetadataDumperUsageException(
          "A password prompt was requested, but there is no console available.");
    }
    console.printf("Password: ");
    cachedValue = new String(console.readPassword());
    hasCachedValue = true;
    return cachedValue;
  }
}
