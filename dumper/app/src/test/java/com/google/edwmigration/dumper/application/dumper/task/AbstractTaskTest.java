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
package com.google.edwmigration.dumper.application.dumper.task;

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.AbstractHandle;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/** @author shevek */
public abstract class AbstractTaskTest {

  protected static class DummyHandle extends AbstractHandle {}

  protected static Handle HANDLE = new DummyHandle();

  protected static class MemoryByteSink extends ByteSink {

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    @Override
    public OutputStream openStream() throws IOException {
      return out;
    }
  }
}
