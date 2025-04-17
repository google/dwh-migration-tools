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
package com.google.edwmigration.dumper.application.dumper.io;

import com.google.common.io.ByteSink;
import com.google.common.io.CharSink;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import javax.annotation.Nonnull;

/**
 * Enables --continue by allowing a write-temp-and-commit protocol.
 *
 * @author shevek
 */
public interface OutputHandle extends Closeable {

  enum WriteMode {
    CREATE_TRUNCATE, // Create a new output or truncate an existing output. Default.
    APPEND_EXISTING // Append to an existing output.
  }

  public boolean exists() throws IOException;

  /** Returns a ByteSink on the target file. */
  @Nonnull
  public ByteSink asByteSink(@Nonnull WriteMode writeMode) throws IOException;

  /** Returns a CharSink on the target file. */
  @Nonnull
  public default CharSink asCharSink(@Nonnull Charset charset, @Nonnull WriteMode writeMode)
      throws IOException {
    return asByteSink(writeMode).asCharSink(charset);
  }

  /** Returns a ByteSink on the temporary file. */
  @Nonnull
  public ByteSink asTemporaryByteSink(@Nonnull WriteMode writeMode) throws IOException;

  @Nonnull
  default ByteSink asTemporaryByteSink() throws IOException {
    return asTemporaryByteSink(WriteMode.CREATE_TRUNCATE);
  }

  /**
   * Renames the temporary file to the final file.
   *
   * <p>The stream, if any, must be closed. The temporary file must exist. If the instance of this
   * class is created in a try-with-resources call or this method is used in finally block, then
   * make sure to have the underlying stream closed before.
   */
  public void commit() throws IOException;

  @Override
  default void close() throws IOException {
    commit();
  }
}
