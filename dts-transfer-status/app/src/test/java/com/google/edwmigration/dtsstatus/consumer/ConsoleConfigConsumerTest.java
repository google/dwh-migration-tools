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
package com.google.edwmigration.dtsstatus.consumer;

import static com.google.common.truth.Truth.assertThat;

import com.google.edwmigration.dtsstatus.model.TransferConfig;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConsoleConfigConsumerTest {
  private final ByteArrayOutputStream printedContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;

  @BeforeEach
  public void setUpStreams() {
    System.setOut(new PrintStream(printedContent));
  }

  @AfterEach
  public void restoreStreams() {
    System.setOut(originalOut);
  }

  @Test
  public void noData_messageDisplayed() {
    try (ConsoleConfigConsumer consumer = new ConsoleConfigConsumer()) {}

    assertThat(printedContent.toString()).isEqualTo("No data to display." + System.lineSeparator());
  }

  @Test
  public void singleConfig_tableDisplayed() {
    try (ConsoleConfigConsumer consumer = new ConsoleConfigConsumer()) {
      consumer.accept(new TransferConfig("aa", "bb"));
    }

    String printedContentString = printedContent.toString();
    assertThat(printedContentString).contains("aa");
    assertThat(printedContentString).contains("bb");
  }
}
