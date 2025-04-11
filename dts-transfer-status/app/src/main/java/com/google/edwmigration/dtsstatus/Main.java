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
package com.google.edwmigration.dtsstatus;

import com.google.edwmigration.dtsstatus.command.ListStatusForConfig;
import com.google.edwmigration.dtsstatus.command.ListStatusForDatabase;
import com.google.edwmigration.dtsstatus.command.ListTransferConfigs;
import com.google.edwmigration.dtsstatus.consumer.ConfigConsumerFactory;
import com.google.edwmigration.dtsstatus.consumer.StatusConsumerFactory;
import com.google.edwmigration.dtsstatus.consumer.TransferConfigConsumer;
import com.google.edwmigration.dtsstatus.consumer.TransferStatusConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    try {
      StatusOptions options = new StatusOptions(args);
      if (options.hasListTransferConfigs()) {
        try (TransferConfigConsumer consumer = ConfigConsumerFactory.create(options)) {
          ListTransferConfigs.instance(options, consumer).run();
        }
      } else if (options.hasListStatusForConfig()) {
        try (TransferStatusConsumer consumer = StatusConsumerFactory.create(options)) {
          ListStatusForConfig.instance(options, consumer).run();
        }
      } else if (options.hasListStatusForDatabase()) {
        try (TransferStatusConsumer consumer = StatusConsumerFactory.create(options)) {
          ListStatusForDatabase.instance(options, consumer).run();
        }
      } else {
        logger.error("No command specified");
      }
    } catch (Throwable ex) {
      logger.error("Error while performing the requested action: {}", ex.getMessage());
    }
  }
}
