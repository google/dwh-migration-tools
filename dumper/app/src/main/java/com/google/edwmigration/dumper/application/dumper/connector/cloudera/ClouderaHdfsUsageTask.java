package com.google.edwmigration.dumper.application.dumper.connector.cloudera;

import com.cloudera.api.swagger.ClustersResourceApi;
import com.cloudera.api.swagger.ServicesResourceApi;
import com.cloudera.api.swagger.model.ApiCluster;
import com.cloudera.api.swagger.model.ApiClusterList;
import com.cloudera.api.swagger.model.ApiHdfsUsageReport;
import com.cloudera.api.swagger.model.ApiService;
import com.cloudera.api.swagger.model.ApiServiceList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.CoreMetadataDumpFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

public class ClouderaHdfsUsageTask extends AbstractClouderaTask{

  public ClouderaHdfsUsageTask() {
    super("clusters.json");
  }

  @CheckForNull
  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    ClouderaHandle h = (ClouderaHandle) handle;
    ApiClusterList clusters = getClusters(h);
    ServicesResourceApi api = new ServicesResourceApi(h.getClient());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      for (ApiCluster cluster : clusters.getItems()) {
        ApiServiceList services = api.readServices(cluster.getName(), null);
        for (ApiService service : services.getItems()) {
          ApiHdfsUsageReport report = api.getHdfsUsageReport(cluster.getName(), service.getName(), null, null, null, null);
          // TODO: Dump JSONL.
        }
      }
      // CoreMetadataDumpFormat.MAPPER.writeValue(writer, list);
    }
    return null;
  }
}
