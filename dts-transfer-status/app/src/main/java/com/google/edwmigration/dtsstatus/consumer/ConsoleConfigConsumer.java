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

import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.Column;
import com.google.edwmigration.dtsstatus.model.TransferConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link TransferConfigConsumer} that prints the data to the console. The data is printed in
 * table format. Data is flushed to the screen after a set number of rows.
 */
public class ConsoleConfigConsumer implements TransferConfigConsumer {

  private final List<TransferConfig> data = new ArrayList<>();
  private boolean hadData = false;
  private static final int MAX_ROWS = 100;

  @Override
  public void accept(TransferConfig transferConfig) {
    data.add(transferConfig);
    hadData = true;
    if (data.size() > MAX_ROWS) {
      flush();
    }
  }

  @Override
  public void close() {
    if (!data.isEmpty()) {
      flush();
    } else if (!hadData) {
      System.out.println("No data to display.");
    }
  }

  private void flush() {
    String table =
        AsciiTable.getTable(
            data,
            Arrays.asList(
                new Column().header("Config ID").with(TransferConfig::getConfigId),
                new Column().header("Name").with(TransferConfig::getName)));
    System.out.println(table);
    data.clear();
  }
}
