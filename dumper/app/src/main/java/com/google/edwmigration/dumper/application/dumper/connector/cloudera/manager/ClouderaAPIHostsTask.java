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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager;

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

/**
 * The tasks dum data from the <a
 * href="https://archive.cloudera.com/cm7/7.11.3.0/generic/jar/cm_api/apidocs/json_ApiHostList.html">Hosts
 * API</a> which doesn't contain usage and disk data and collected as a fallback.
 */
public class ClouderaAPIHostsTask extends AbstractClouderaManagerTask {

  public ClouderaAPIHostsTask() {
    super("api-hosts.jsonl");
  }

  @Override
  protected Void doRun(
      TaskRunContext context, @Nonnull ByteSink sink, @Nonnull ClouderaManagerHandle handle)
      throws Exception {
    CloseableHttpClient httpClient = handle.getHttpClient();
    List<ClouderaClusterDTO> clusters = handle.getClusters();
    if (clusters == null) {
      throw new MetadataDumperUsageException(
          "Cloudera clusters must be initialized before hosts dumping.");
    }

    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      for (ClouderaClusterDTO cluster : clusters) {
        String hostPerClusterUrl = handle.getApiURI() + "/clusters/" + cluster.getName() + "/hosts";

        try (CloseableHttpResponse hosts = httpClient.execute(new HttpGet(hostPerClusterUrl))) {
          String json = EntityUtils.toString(hosts.getEntity());
          writer.write(jsonToJsonl(json));
          writer.write('\n');
        }
      }
    }
    return null;
  }
}
