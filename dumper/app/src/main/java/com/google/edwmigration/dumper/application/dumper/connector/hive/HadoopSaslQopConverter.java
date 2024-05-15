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
package com.google.edwmigration.dumper.application.dumper.connector.hive;

import static com.google.edwmigration.dumper.application.dumper.utils.OptionalUtils.optionallyIfNotEmpty;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;

import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.hive.AbstractHiveConnector.HiveConnectorProperty;
import java.util.Optional;

public class HadoopSaslQopConverter {

  enum HadoopRpcProtection {
    AUTHENTICATION("auth"),
    INTEGRITY("auth-int"),
    PRIVACY("auth-conf");

    static final String ALLOWED_VALUES =
        stream(HadoopRpcProtection.values())
            .map(protection -> protection.name().toLowerCase())
            .collect(joining(", "));

    private final String qopValue;

    HadoopRpcProtection(String qopValue) {
      this.qopValue = qopValue;
    }
  }

  static Optional<String> convert(String value) throws MetadataDumperUsageException {
    return optionallyIfNotEmpty(value).map(HadoopSaslQopConverter::convertInternal);
  }

  private static String convertInternal(String value) {
    return stream(HadoopRpcProtection.values())
        .filter(qop -> qop.name().equalsIgnoreCase(value))
        .findFirst()
        .map(qop -> qop.qopValue)
        .orElseThrow(
            () ->
                new MetadataDumperUsageException(
                    String.format(
                        "Invalid value '%s' for property '%s'. Allowed values are: '%s'.",
                        value,
                        HiveConnectorProperty.RPC_PROTECTION.getName(),
                        HadoopRpcProtection.ALLOWED_VALUES)));
  }
}
