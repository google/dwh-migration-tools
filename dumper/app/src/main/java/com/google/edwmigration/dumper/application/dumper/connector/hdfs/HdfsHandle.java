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
package com.google.edwmigration.dumper.application.dumper.connector.hdfs;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_HADOOP_CORE_SITE_XML;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_HADOOP_HDFS_SITE_XML;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_KERBEROS_PRINCIPAL;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.login.LoginException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HdfsHandle implements Handle {
  private static final Logger logger = LoggerFactory.getLogger(HdfsHandle.class);

  private final String clusterHost;
  private final int port;
  private final DistributedFileSystem dfs;

  HdfsHandle(@Nonnull ConnectorArguments args) throws IOException {
    clusterHost = args.getHostOrDefault();
    port = args.getPort(/* defaultPort= */ 8020);

    logger.info("clusterHost: '{}'", clusterHost);
    logger.info("port: '{}'", port);

    Configuration conf = new Configuration();
    if (args.useKerberosAuthForHadoop()) {
      set(conf, "hadoop.security.authentication", "kerberos");

      boolean coreSiteXmlSpecified = args.has(args.optionHadoopCoreSiteXml);
      if (coreSiteXmlSpecified) {
        String coreSiteXml = args.getHadoopCoreSiteXml();
        logger.info("core-site.xml path specified: {}", coreSiteXml);
        Preconditions.checkArgument(
            Files.exists(Paths.get(coreSiteXml)),
            "Argument --" + OPT_HADOOP_CORE_SITE_XML + " specifies invalid path: " + coreSiteXml);
        conf.addResource(new Path(coreSiteXml));
      }

      boolean hdfsSiteXmlSpecified = args.has(args.optionHadoopHdfsSiteXml);
      if (hdfsSiteXmlSpecified) {
        String hdfsSiteXml = args.getHadoopHdfsSiteXml();
        logger.info("hdfs-site.xml path specified: {}", hdfsSiteXml);
        Preconditions.checkArgument(
            Files.exists(Paths.get(hdfsSiteXml)),
            "Argument --" + OPT_HADOOP_HDFS_SITE_XML + " specifies invalid path: " + hdfsSiteXml);
        conf.addResource(new Path(hdfsSiteXml));
      }

      String krbPrincipal = args.getKerberosPrincipal();
      String krbKeytab = args.getKerberosKeytabPath();
      if (krbKeytab != null) {
        logger.info("Kerberos keytab path : {}", krbKeytab);
        // keytab requires principal:
        Preconditions.checkArgument(
            krbPrincipal != null, "Missing argument --" + OPT_KERBEROS_PRINCIPAL);
        logger.info("Kerberos principal : {}", krbPrincipal);

        if (!coreSiteXmlSpecified) {
          set(conf, "hadoop.security.authorization", "true");
        }

        String rpcProtection = args.getHadoopRpcProtection();
        if (rpcProtection != null) {
          // Override default protection:
          set(conf, "hadoop.rpc.protection", rpcProtection);
        }

        String[] hdfsPrincipals =
            new String[] {"dfs.namenode.kerberos.principal", "dfs.datanode.kerberos.principal"};
        if (!hdfsSiteXmlSpecified) {
          // Need to infer hdfsPrincipal pattern if hdfs-site.xml was not included:
          String krbRealm = extractKerberosRealmFromPrincipal(krbPrincipal);
          Preconditions.checkArgument(
              krbRealm != null,
              "Argument --" + OPT_KERBEROS_PRINCIPAL + " missing @<REALM> suffix: " + krbPrincipal);
          logger.info("Kerberos realm (extracted from Kerberos principal): {}", krbRealm);
          String hdfsPrincipal = args.getHdfsPrincipalPrefix() + krbRealm;
          logger.info("Hdfs principal (constructed from Kerberos realm): {}", hdfsPrincipal);
          for (String prop : hdfsPrincipals) {
            conf.set(prop, hdfsPrincipal);
          }
        }
        for (String prop : hdfsPrincipals) {
          logger.info("{}: {}", prop, conf.get(prop));
        }

        // Auth by provided principal and its keytab:
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(krbPrincipal, krbKeytab);
      } else { // No keytab:
        // Auth by credentials cached by last invocation of `kinit`:
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromSubject(null);
      }
    } else {
      ensureLoginUser();
    }
    set(conf, "fs.defaultFS", "hdfs://" + clusterHost + ":" + port + "/");
    set(conf, "fs.hdfs.impl", DistributedFileSystem.class.getName());
    FileSystem fs = FileSystem.get(conf);
    checkArgument(fs instanceof DistributedFileSystem, "Not a DistributedFileSystem");
    dfs = (DistributedFileSystem) fs;
  }

  /**
   * Makes sure that Hadoop has a login user when Kerberos is not used.
   *
   * <p>Hadoop takes the user from the operating system first, and fails with a {@link
   * LoginException} cause when the UID of this process has no user name, as in a container that
   * runs with a UID that is not in its passwd file. {@code HADOOP_USER_NAME} is not read in that
   * case. The user is then taken from the environment or system properties, see {@link
   * #fallbackUserName}. Other login errors, such as an unreadable token file, are rethrown.
   */
  private static void ensureLoginUser() throws IOException {
    try {
      UserGroupInformation.getLoginUser();
    } catch (IOException e) {
      if (UserGroupInformation.isSecurityEnabled() || !isLoginFailure(e)) {
        throw e;
      }
      String userName =
          fallbackUserName(
              System.getenv("HADOOP_USER_NAME"),
              System.getProperty("HADOOP_USER_NAME"),
              System.getenv("USER"),
              System.getProperty("user.name"));
      if (userName == null) {
        throw new MetadataDumperUsageException(
            String.format(
                "Hadoop could not determine the user of this process (%s). Set the"
                    + " HADOOP_USER_NAME environment variable to the user to access HDFS as.",
                e.getMessage()));
      }
      logger.warn(
          "Hadoop could not determine the user of this process ({}). Accessing HDFS as '{}'.",
          e.getMessage(),
          userName);
      UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(userName));
    }
  }

  /**
   * Returns whether the exception wraps a JAAS {@link LoginException} from Hadoop's OS user lookup,
   * rather than a later failure such as reading a token file.
   */
  @VisibleForTesting
  static boolean isLoginFailure(@Nonnull IOException exception) {
    return exception.getCause() instanceof LoginException;
  }

  /**
   * Returns the first usable name of: the HADOOP_USER_NAME environment variable, the
   * HADOOP_USER_NAME system property, the USER environment variable, and the user.name system
   * property, which Java sets to "?" for a UID without a user name. Returns null if there is none.
   */
  @VisibleForTesting
  @CheckForNull
  static String fallbackUserName(
      @Nullable String hadoopUserNameEnv,
      @Nullable String hadoopUserNameProperty,
      @Nullable String user,
      @Nullable String userNameProperty) {
    if (!Strings.isNullOrEmpty(hadoopUserNameEnv)) {
      return hadoopUserNameEnv;
    }
    if (!Strings.isNullOrEmpty(hadoopUserNameProperty)) {
      return hadoopUserNameProperty;
    }
    if (!Strings.isNullOrEmpty(user)) {
      return user;
    }
    if (!Strings.isNullOrEmpty(userNameProperty) && !"?".equals(userNameProperty)) {
      return userNameProperty;
    }
    return null;
  }

  private static void set(Configuration conf, String property, String value) {
    logger.info("{}: {}", property, value);
    conf.set(property, value);
  }

  private static String extractKerberosRealmFromPrincipal(String krbPrincipal) {
    int startIdx = krbPrincipal.lastIndexOf('@');
    if (startIdx >= 0 && startIdx + 1 < krbPrincipal.length()) {
      return krbPrincipal.substring(startIdx + 1);
    }
    return null;
  }

  DistributedFileSystem getDfs() {
    return dfs;
  }

  @Override
  public void close() throws IOException {
    // No need to close dfs - it's read only accessed by multiple Tasks
    // and then the process exits.
  }
}
