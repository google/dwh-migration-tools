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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_ASSESSMENT;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_DATABASE;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_PRIVATE_KEY_PASSWORD;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_QUERY_LOG_EARLIEST_TIMESTAMP;
import static java.util.stream.Collectors.joining;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
class SnowflakeUsageException extends MetadataDumperUsageException {

  @Override
  @Nonnull
  public final ImmutableList<String> getMessages() {
    return ImmutableList.of();
  }

  private SnowflakeUsageException(String msg) {
    super(msg);
  }

  @Override
  @Nonnull
  public String toString() {
    String className = getClass().getName();
    return String.format("%s: %s", className, getLocalizedMessage());
  }

  @Nonnull
  static SnowflakeUsageException missingAssessmentException(String connector) {
    Stream<String> messages =
        Stream.of(
            "The " + connector + " connector only supports extraction for assessment.",
            "Provide the '--" + OPT_ASSESSMENT + " flag to use this connector.");
    return new SnowflakeUsageException(messages.collect(joining(" ")));
  }

  static SnowflakeUsageException mixedAuthentication() {
    Stream<String> messages =
        Stream.of(
            "Private key authentication method can't be used together with user password.",
            "If the private key file is encrypted, please use --"
                + OPT_PRIVATE_KEY_PASSWORD
                + " to specify the key password.");
    return new SnowflakeUsageException(messages.collect(joining(" ")));
  }

  @Nonnull
  static SnowflakeUsageException unsupportedAssessment() {
    String message = String.format("The --%s flag is not supported.", OPT_ASSESSMENT);
    return new SnowflakeUsageException(message);
  }

  @Nonnull
  static SnowflakeUsageException unsupportedEarliestTimestamp() {
    String message =
        String.format(
            "Unsupported option used with --%s: please remove --%s",
            OPT_ASSESSMENT, OPT_QUERY_LOG_EARLIEST_TIMESTAMP);
    return new SnowflakeUsageException(message);
  }

  @Nonnull
  static SnowflakeUsageException unsupportedFilter() {
    Stream<String> messages =
        Stream.of(
            "Trying to filter by database with the --" + OPT_ASSESSMENT + " flag.",
            "This is unsupported in Assessment.",
            "Remove either the --" + OPT_ASSESSMENT + " or the --" + OPT_DATABASE + " flag.");
    return new SnowflakeUsageException(messages.collect(joining(" ")));
  }
}
