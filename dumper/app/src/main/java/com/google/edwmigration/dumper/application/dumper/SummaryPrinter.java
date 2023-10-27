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
package com.google.edwmigration.dumper.application.dumper;

import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;

public class SummaryPrinter {

  private static final String STARS = StringUtils.repeat('*', 68);
  private boolean separatorPrinted;

  public void printSummarySection(String message) {
    printSummarySection(linePrinter -> linePrinter.println(message));
  }

  public void printSummarySection(Consumer<SummaryLinePrinter> sectionGenerator) {
    if (!separatorPrinted) {
      separatorPrinted = true;
      printSeparator();
    }
    sectionGenerator.accept(this::printSummaryLine);
    printSeparator();
  }

  private void printSummaryLine(String message) {
    System.out.println("* " + message);
  }

  private void printSeparator() {
    System.out.println(STARS);
  }

  @FunctionalInterface
  public interface SummaryLinePrinter {

    void println(String s);

    default void println(String s, Object... parameters) {
      println(String.format(s, parameters));
    }
  }
}
