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
package com.google.edwmigration.permissions;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;

/** Exception thrown when there's a problem converting a YAML stream to a protobuf. */
public class YamlConversionException extends RuntimeException {

  YamlConversionException(String message, Throwable cause) {
    super(message, cause);
  }

  YamlConversionException(String message) {
    super(message);
  }

  @FormatMethod
  static YamlConversionException formatMessage(@FormatString String message, Object... args) {
    return new YamlConversionException(String.format(message, args));
  }

  @FormatMethod
  static YamlConversionException formatMessage(
      Throwable cause, @FormatString String message, Object... args) {
    return new YamlConversionException(String.format(message, args), cause);
  }
}
