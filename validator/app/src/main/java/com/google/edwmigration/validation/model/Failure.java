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
package com.google.edwmigration.validation.model;

import java.util.Objects;

public class Failure {

  public enum Kind {
    IO,
    CONFIG,
    VALIDATION,
    GCS_UPLOAD,
    SQL,
    UNKNOWN;

    public Failure with(String message) {
      return new Failure(this, message);
    }

    public Failure with(String message, Throwable cause) {
      return new Failure(this, message, cause);
    }
  }

  public final Kind kind;
  public final String message;
  public final Throwable cause;

  public static final Kind IO = Kind.IO;
  public static final Kind CONFIG = Kind.CONFIG;
  public static final Kind VALIDATION = Kind.VALIDATION;
  public static final Kind GCS_UPLOAD = Kind.GCS_UPLOAD;
  public static final Kind SQL = Kind.SQL;
  public static final Kind UNKNOWN = Kind.UNKNOWN;

  private Failure(Kind kind, String message, Throwable cause) {
    this.kind = Objects.requireNonNull(kind);
    this.message = Objects.requireNonNull(message);
    this.cause = cause;
  }

  private Failure(Kind kind, String message) {
    this(kind, message, null);
  }

  @Override
  public String toString() {
    return "[" + kind + "] " + message + (cause != null ? "\nCaused by: " + cause : "");
  }
}
