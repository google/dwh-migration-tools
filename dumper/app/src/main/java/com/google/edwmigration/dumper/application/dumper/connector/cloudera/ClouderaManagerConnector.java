package com.google.edwmigration.dumper.application.dumper.connector.cloudera;

import com.cloudera.api.swagger.ClustersResourceApi;
import com.cloudera.api.swagger.client.ApiClient;
import com.cloudera.api.swagger.client.Configuration;
import com.google.auto.service.AutoService;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentPassword;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogDays;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogEnd;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogStart;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentUri;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentUser;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnector;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import java.util.List;
import javax.annotation.Nonnull;

@AutoService({Connector.class})
@Description("Dumps metadata from Cloudera Manager.")
@RespectsArgumentUser
@RespectsArgumentPassword
@RespectsArgumentUri
public class ClouderaManagerConnector extends AbstractConnector {
  public static final String FORMAT_NAME = "cloudera-manager.dump.zip";

  public ClouderaManagerConnector() {
    super("cloudera-manager");
  }

  @Nonnull
  @Override
  public String getDefaultFileName(boolean isAssessment) {
    return "dwh-migration-cloudera-manager-dump.zip";
  }

  @Nonnull
  @Override
  public void addTasksTo(@Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws Exception {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));
    out.add(new ClouderaHostsTask());
    out.add(new ClouderaClustersTask());
  }

  @Nonnull
  @Override
  public Handle open(@Nonnull ConnectorArguments arguments) throws Exception {
    ApiClient cmClient = Configuration.getDefaultApiClient();

    // Configure HTTP basic authorization: basic
    cmClient.setBasePath(arguments.getUri());
    cmClient.setUsername(arguments.getUser());
    cmClient.setPassword(arguments.getPassword());

    // Configure TLS for secure communication
    cmClient.setVerifyingSsl(true);

    // Path truststorePath = Paths.get("/path/to/ca_cert_file.pem");
    // byte[] truststoreBytes = Files.readAllBytes(truststorePath);
    // cmClient.setSslCaCert(new ByteArrayInputStream(truststoreBytes));

    ClustersResourceApi apiInstance = new ClustersResourceApi(cmClient);

    return new ClouderaHandle(cmClient);
  }
}
