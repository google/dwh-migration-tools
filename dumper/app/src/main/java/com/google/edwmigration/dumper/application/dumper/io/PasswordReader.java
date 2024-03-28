package com.google.edwmigration.dumper.application.dumper.io;

import java.io.Console;

import javax.annotation.Nonnull;

import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;

// Provides the functionality of reading a password from prompt.
//
// Needed, because users may not want to provide the password on the command line.
// This might be for security reasons or due to issues caused by special characters.
public class PasswordReader {

  private boolean hasCachedValue = false;
  @Nonnull private String cachedValue = "";

  @Nonnull
  public String getOrPrompt() {
    if (hasCachedValue) {
      return cachedValue;
    }
    Console console = System.console();
    if (console == null) {
      // user requested a prompt, but we can't even get a console, so let's just fail
      // - continuing with a null password is almost certainly not what the user wants here
      throw new MetadataDumperUsageException(
        "A password prompt was requested, but there is no console available.");
    }
    console.printf("Password: ");
    cachedValue = new String(console.readPassword());
    hasCachedValue = true;
    return cachedValue;
  }
}
