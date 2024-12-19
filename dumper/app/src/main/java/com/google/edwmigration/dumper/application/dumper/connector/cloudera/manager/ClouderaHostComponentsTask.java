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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaHostDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

/**
 * The task dumps data from the <a
 * href="https://archive.cloudera.com/cm7/7.11.3.0/generic/jar/cm_api/apidocs/resource_HostsResource.html#resource_HostsResource_HostsResourceV55_getHostComponents_GET">Hosts
 * components API</a>
 */
public class ClouderaHostComponentsTask extends AbstractClouderaManagerTask {

  public ClouderaHostComponentsTask() {
    super("host-components.jsonl");
  }

  @Override
  protected void doRun(
      TaskRunContext context, @Nonnull ByteSink sink, @Nonnull ClouderaManagerHandle handle)
      throws Exception {
    CloseableHttpClient httpClient = handle.getHttpClient();
    List<ClouderaHostDTO> hosts = handle.getHosts();
    if (hosts == null) {
      throw new MetadataDumperUsageException(
          "Cloudera hosts must be initialized before Host's components dumping.");
    }

    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      for (ClouderaHostDTO host : hosts) {
        String hostsComponentsUrl = handle.getApiURI() + "/hosts/" + host.getId() + "/components";

        try (CloseableHttpResponse response = httpClient.execute(new HttpGet(hostsComponentsUrl))) {
          JsonNode json = getObjectMapper().readTree(response.getEntity().getContent());
          writer.write(json.toString());
          writer.write('\n');
        }
      }
    }
  }
}
