package com.google.edwmigration.dumper.application.dumper.connector.cloudera;

import com.cloudera.api.swagger.AllHostsResourceApi;
import com.cloudera.api.swagger.HostsResourceApi;
import com.cloudera.api.swagger.client.ApiClient;
import com.cloudera.api.swagger.model.ApiHost;
import com.cloudera.api.swagger.model.ApiHostList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.CoreMetadataDumpFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

public class ClouderaHostsTask extends AbstractClouderaTask{

  public ClouderaHostsTask() {
    super("hosts.json");
  }

  @CheckForNull
  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    ClouderaHandle h = (ClouderaHandle) handle;
    HostsResourceApi api = new HostsResourceApi(h.getClient());
    ApiHostList list = api.readHosts(null, null, null);
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      CoreMetadataDumpFormat.MAPPER.writeValue(writer, list);
    }
    return null;
  }
}
