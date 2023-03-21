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

import com.google.common.io.ByteSink;
import com.google.common.io.CharSink;
import java.io.IOException;
import java.nio.charset.Charset;
import javax.annotation.Nonnull;

/**
 * Enables --continue by allowing a write-temp-and-commit protocol.
 *
 * @author shevek
 */
public interface OutputHandle {

  public boolean exists() throws IOException;

  /** Returns a ByteSink on the target file. */
  @Nonnull
  public ByteSink asByteSink() throws IOException;

  /** Returns a CharSink on the target file. */
  @Nonnull
  public default CharSink asCharSink(@Nonnull Charset charset) throws IOException {
    return asByteSink().asCharSink(charset);
  }

  /** Returns a ByteSink on the temporary file. */
  @Nonnull
  public ByteSink asTemporaryByteSink() throws IOException;

  /**
   * Renames the temporary file to the final file.
   *
   * <p>The stream, if any, must be closed. The temporary file must exist. Do NOT call commit() in a
   * try-with-resources or finally block, or you risk committing bad data.
   */
  public void commit() throws IOException;
}
