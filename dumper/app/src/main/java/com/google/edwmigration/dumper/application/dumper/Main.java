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
package com.google.edwmigration.dumper.application.dumper;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author miguel */
public class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  private static void printErrorMessages(Throwable e) {
    new SummaryPrinter()
        .printSummarySection(
            linePrinter -> {
              linePrinter.println("ERROR");
              ImmutableList<String> errorMessages =
                  Throwables.getCausalChain(e).stream()
                      .map(Throwable::getMessage)
                      .filter(Objects::nonNull)
                      .collect(toImmutableList());
              for (int i = 0; i < errorMessages.size(); i++) {
                String errorMessage = errorMessages.get(i);
                if (i > 0) {
                  errorMessage = "Caused by: " + errorMessage;
                }
                linePrinter.println(errorMessage);
              }
            });
  }

  public static void main(String... args) throws Exception {
    try {
      StartUpMetaInfoProcessor.printMetaInfo();

      if (args.length == 0) {
        args = new String[] {"--help"};
      }

      MetadataDumper metadataDumper = new MetadataDumper(args);

      if (!metadataDumper.run()) {
        System.exit(1);
      }
    } catch (MetadataDumperUsageException e) {
      logger.error(e.getMessage());
      for (String msg : e.getMessages()) {
        logger.error(msg);
      }
      System.exit(1);
    } catch (Exception e) {
      e.printStackTrace();
      printErrorMessages(e);
      System.exit(1);
    }
  }
}
