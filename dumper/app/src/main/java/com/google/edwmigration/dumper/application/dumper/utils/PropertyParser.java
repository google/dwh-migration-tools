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
package com.google.edwmigration.dumper.application.dumper.utils;

import static com.google.edwmigration.dumper.application.dumper.utils.OptionalUtils.optionallyIfNotEmpty;

import com.google.common.collect.Range;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.commons.lang3.StringUtils;

/**
 * Parser of the command-line options specified with <code>-D</code> prefix, represented in code as
 * {@link ConnectorProperty} classes.
 */
public class PropertyParser {

  /**
   * Parses a number passed as an argument to the command-line option and validates that the value
   * is within the specified range.
   *
   * @param arguments all command-line options
   * @param property command-line option
   * @param allowedRange range to validate the number against
   * @return the parsed number or empty, if the option was not provided on the command-line
   * @throws MetadataDumperUsageException in case of parsing or validation error
   */
  public static OptionalLong parseNumber(
      ConnectorArguments arguments, ConnectorProperty property, Range<Long> allowedRange)
      throws MetadataDumperUsageException {
    String stringValue = arguments.getDefinition(property);
    if (StringUtils.isEmpty(stringValue)) {
      return OptionalLong.empty();
    }
    long value;
    try {
      value = Long.parseLong(stringValue);
    } catch (NumberFormatException ex) {
      throw new MetadataDumperUsageException(
          createErrorMessage(stringValue, property, allowedRange));
    }
    if (!allowedRange.contains(value)) {
      throw new MetadataDumperUsageException(
          createErrorMessage(stringValue, property, allowedRange));
    }
    return OptionalLong.of(value);
  }

  /**
   * Gets the string value passed as an argument to the command-line option.
   *
   * @param arguments all command-line options
   * @param property command-line option
   * @return the string value of the argument or an empty optional
   */
  public static Optional<String> getString(
      ConnectorArguments arguments, ConnectorProperty property) {
    return optionallyIfNotEmpty(arguments.getDefinition(property));
  }

  private static String createErrorMessage(
      String stringValue, ConnectorProperty property, Range<Long> allowedRange) {
    StringBuilder errorMessage = new StringBuilder();
    errorMessage
        .append("ERROR: Option '")
        .append(property.getName())
        .append("' accepts only integers");
    if (!allowedRange.isEmpty()) {
      errorMessage.append(" in range ").append(allowedRange);
    }
    errorMessage.append(". Actual: '").append(stringValue).append("'.");
    return errorMessage.toString();
  }
}
