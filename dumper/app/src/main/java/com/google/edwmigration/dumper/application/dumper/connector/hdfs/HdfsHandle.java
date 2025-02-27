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

import com.google.common.base.Preconditions;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.annotation.Nonnull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HdfsHandle implements Handle {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsHandle.class);

  private final String clusterHost;
  private final int port;
  private final DistributedFileSystem dfs;

  HdfsHandle(@Nonnull ConnectorArguments args) throws IOException {
    clusterHost = args.getHostOrDefault();
    port = args.getPort(/* defaultPort= */ 8020);

    LOG.info("clusterHost: '{}'", clusterHost);
    LOG.info("port: '{}'", port);

    Configuration conf = new Configuration();
    if (args.useKerberosAuthForHadoop()) {
      set(conf, "hadoop.security.authentication", "kerberos");

      boolean coreSiteXmlSpecified = args.has(args.optionHadoopCoreSiteXml);
      if (coreSiteXmlSpecified) {
        String coreSiteXml = args.getHadoopCoreSiteXml();
        LOG.info("core-site.xml path specified: {}", coreSiteXml);
        Preconditions.checkArgument(
            Files.exists(Paths.get(coreSiteXml)),
            "Argument --" + OPT_HADOOP_CORE_SITE_XML + " specifies invalid path: " + coreSiteXml);
        conf.addResource(new Path(coreSiteXml));
      }

      boolean hdfsSiteXmlSpecified = args.has(args.optionHadoopHdfsSiteXml);
      if (hdfsSiteXmlSpecified) {
        String hdfsSiteXml = args.getHadoopHdfsSiteXml();
        LOG.info("hdfs-site.xml path specified: {}", hdfsSiteXml);
        Preconditions.checkArgument(
            Files.exists(Paths.get(hdfsSiteXml)),
            "Argument --" + OPT_HADOOP_HDFS_SITE_XML + " specifies invalid path: " + hdfsSiteXml);
        conf.addResource(new Path(hdfsSiteXml));
      }

      String krbPrincipal = args.getKerberosPrincipal();
      String krbKeytab = args.getKerberosKeytabPath();
      if (krbKeytab != null) {
        LOG.info("Kerberos keytab path : {}", krbKeytab);
        // keytab requires principal:
        Preconditions.checkArgument(
            krbPrincipal != null, "Missing argument --" + OPT_KERBEROS_PRINCIPAL);
        LOG.info("Kerberos principal : {}", krbPrincipal);

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
          LOG.info("Kerberos realm (extracted from Kerberos principal): {}", krbRealm);
          String hdfsPrincipal = args.getHdfsPrincipalPrefix() + krbRealm;
          LOG.info("Hdfs principal (constructed from Kerberos realm): {}", hdfsPrincipal);
          for (String prop : hdfsPrincipals) {
            conf.set(prop, hdfsPrincipal);
          }
        }
        for (String prop : hdfsPrincipals) {
          LOG.info("{}: {}", prop, conf.get(prop));
        }

        // Auth by provided principal and its keytab:
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(krbPrincipal, krbKeytab);
      } else { // No keytab:
        // Auth by credentials cached by last invocation of `kinit`:
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromSubject(null);
      }
    }
    set(conf, "fs.defaultFS", "hdfs://" + clusterHost + ":" + port + "/");
    set(conf, "fs.hdfs.impl", DistributedFileSystem.class.getName());
    FileSystem fs = FileSystem.get(conf);
    checkArgument(fs instanceof DistributedFileSystem, "Not a DistributedFileSystem");
    dfs = (DistributedFileSystem) fs;
  }

  private static void set(Configuration conf, String property, String value) {
    LOG.info("{}: {}", property, value);
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
