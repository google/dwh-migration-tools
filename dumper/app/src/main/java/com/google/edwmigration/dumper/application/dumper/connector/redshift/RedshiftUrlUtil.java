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
package com.google.edwmigration.dumper.application.dumper.connector.redshift;

import com.google.common.collect.Iterables;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.io.UnsupportedEncodingException;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
class RedshiftUrlUtil {

  static final int DEFAULT_PORT = 5439;
  // for annotations
  static final String OPT_PORT_DEFAULT = "5439";

  @Nonnull
  static String makeJdbcUrlPostgresql(ConnectorArguments arguments)
      throws MetadataDumperUsageException, UnsupportedEncodingException {
    String password = arguments.getPasswordIfFlagProvided().orElse(null);
    return "jdbc:postgresql://"
        + arguments.getHostOrDefault()
        + ":"
        + arguments.getPort(DEFAULT_PORT)
        + "/"
        + Iterables.getFirst(arguments.getDatabases(), "")
        + new JdbcPropBuilder("?=&")
            .propOrWarn("user", arguments.getUser(), "--user must be specified")
            .propOrWarn("password", password, "--password must be specified")
            .prop("ssl", "true")
            .toJdbcPart();
  }

  @Nonnull
  static String makeJdbcUrlRedshiftSimple(ConnectorArguments arguments)
      throws MetadataDumperUsageException, UnsupportedEncodingException {
    String password = arguments.getPasswordIfFlagProvided().orElse(null);
    return "jdbc:redshift://"
        + arguments.getHostOrDefault()
        + ":"
        + arguments.getPort(DEFAULT_PORT)
        + "/"
        + Iterables.getFirst(arguments.getDatabases(), "")
        + new JdbcPropBuilder("?=&")
            .propOrWarn("UID", arguments.getUser(), "--user must be specified")
            .propOrWarn("PWD", password, "--password must be specified")
            .toJdbcPart();
  }

  // TODO:  [cluster-id]:[region] syntax.
  // either profile, or key+ secret
  @Nonnull
  static String makeJdbcUrlRedshiftIAM(ConnectorArguments arguments)
      throws UnsupportedEncodingException {
    String url =
        "jdbc:redshift:iam://"
            + arguments.getHostOrDefault()
            + ":"
            + arguments.getPort(DEFAULT_PORT)
            + "/"
            + Iterables.getFirst(arguments.getDatabases(), "");

    if (arguments.getIAMProfile() != null)
      url += new JdbcPropBuilder("?=&").prop("Profile", arguments.getIAMProfile()).toJdbcPart();
    else if (arguments.getIAMAccessKeyID() != null && arguments.getIAMSecretAccessKey() != null)
      url +=
          new JdbcPropBuilder("?=&")
              .prop("AccessKeyID", arguments.getIAMAccessKeyID())
              .prop("SecretAccessKey", arguments.getIAMSecretAccessKey())
              .propOrError("DbUser", arguments.getUser(), "--user must be specified")
              .toJdbcPart();
    // Will use the default IAM from ~/.aws/credentials/
    return url;
  }
}
