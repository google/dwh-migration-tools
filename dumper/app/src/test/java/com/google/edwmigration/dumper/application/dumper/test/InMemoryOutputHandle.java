/*
 * Copyright 2022-2024 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.test;

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTaskTest.MemoryByteSink;
import java.io.IOException;
import javax.annotation.Nonnull;

public class InMemoryOutputHandle implements OutputHandle {

  private final MemoryByteSink memoryByteSink;

  public InMemoryOutputHandle() {
    this.memoryByteSink = new MemoryByteSink();
  }

  @Override
  public boolean exists() throws IOException {
    return false;
  }

  @Nonnull
  @Override
  public ByteSink asByteSink() throws IOException {
    return memoryByteSink;
  }

  @Nonnull
  @Override
  public ByteSink asTemporaryByteSink() throws IOException {
    return memoryByteSink;
  }

  @Override
  public void commit() throws IOException {}

  public String getContent() {
    return memoryByteSink.getContent();
  }
}