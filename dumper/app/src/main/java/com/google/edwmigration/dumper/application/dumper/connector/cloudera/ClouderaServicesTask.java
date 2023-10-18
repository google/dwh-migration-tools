package com.google.edwmigration.dumper.application.dumper.connector.cloudera;

import com.cloudera.api.swagger.ServicesResourceApi;
import com.cloudera.api.swagger.model.*;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ClouderaServicesTask extends AbstractClouderaTask {

    public ClouderaServicesTask() {
        super("services.json");
    }

    // It would be more optimal to combine all metrics that fall under a common parent into a task, so
    // we avoid repeated requests to the same API. For example, HDFS usage can be combined under this
    // task since it pretty much calls the same APIs.
    @CheckForNull
    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
            throws Exception {
        ClouderaHandle h = (ClouderaHandle) handle;
        ApiClusterList clusters = getClusters(h);
        ServicesResourceApi api = new ServicesResourceApi(h.getClient());

        // TODO: Accept startDate as an input. Arbitrarily setting it to today minus 7
        String startDate = LocalDateTime.now().minusDays(7).format(DateTimeFormatter.ISO_DATE);
        try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
            for (ApiCluster cluster : clusters.getItems()) {
                ApiServiceList services = api.readServices(cluster.getName(), null);
                for (ApiService service : services.getItems()) {
                    // TODO: Figure out whether we want to use POOL or USER for tenant type or both
                    ApiYarnUtilization yarnUtilization =
                            api.getYarnUtilization(
                                    cluster.getName(), service.getName(), null, null, startDate, null, null, null);
                    ClouderaServiceObject clouderaServiceObject = new ClouderaServiceObject();
                    clouderaServiceObject.setApiService(service);
                    clouderaServiceObject.setYarnUtilization(yarnUtilization);
                    writer.write(ClouderaConnectorUtils.MAPPER.writeValueAsString(clouderaServiceObject));
                    writer.write("\n");
                }
            }
        }
        return null;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class ClouderaServiceObject {
        ApiService apiService;
        ApiYarnUtilization yarnUtilization;

        public ApiService getApiService() {
            return apiService;
        }

        public void setApiService(ApiService apiService) {
            this.apiService = apiService;
        }

        public ApiYarnUtilization getYarnUtilization() {
            return yarnUtilization;
        }

        public void setYarnUtilization(ApiYarnUtilization yarnUtilization) {
            this.yarnUtilization = yarnUtilization;
        }
    }
}
}
