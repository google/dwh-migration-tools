package com.google.edwmigration.dumper.application.dumper.connector.cloudera;

import com.cloudera.api.swagger.ClustersResourceApi;
import com.cloudera.api.swagger.client.ApiException;
import com.cloudera.api.swagger.model.ApiCluster;
import com.cloudera.api.swagger.model.ApiClusterList;
import com.cloudera.api.swagger.model.ApiClusterUtilization;
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

public class ClouderaClustersTask extends AbstractClouderaTask {

    public ClouderaClustersTask() {
        super("clusters.json");
    }

    @CheckForNull
    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
            throws Exception {
        ClouderaHandle h = (ClouderaHandle) handle;
        ApiClusterList apiClusterList = getClusters(h);
        // TODO: Accept startDate as an input. Arbitrarily setting it to today minus 7
        String startDate = LocalDateTime.now().minusDays(7).format(DateTimeFormatter.ISO_DATE);
        try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
            for (ApiCluster apiCluster : apiClusterList.getItems()) {
                //TODO: We should refactor this so the ClustersResourceApi object is just created once for both retrieving clusters and utilization
                ApiClusterUtilization utilization = getClusterUtilization(h, apiCluster.getName(), startDate);
                ClouderaClusterObject clouderaClusterObject = new ClouderaClusterObject();
                clouderaClusterObject.setCluster(apiCluster);
                clouderaClusterObject.setUtilization(utilization);
                writer.write(ClouderaConnectorUtils.MAPPER.writeValueAsString(clouderaClusterObject));
                writer.write("\n");
            }
        }
        return null;
    }

    @Nonnull
    private ApiClusterUtilization getClusterUtilization(@Nonnull ClouderaHandle handle, String clusterName, String startDate) throws ApiException {
        ClustersResourceApi api = new ClustersResourceApi(handle.getClient());
        return api.getUtilizationReport(clusterName, null, null, startDate, null, null, null);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class ClouderaClusterObject {
        ApiCluster cluster;
        ApiClusterUtilization utilization;

        public ApiCluster getCluster() {
            return cluster;
        }

        public void setCluster(ApiCluster cluster) {
            this.cluster = cluster;
        }

        public ApiClusterUtilization getUtilization() {
            return utilization;
        }

        public void setUtilization(ApiClusterUtilization utilization) {
            this.utilization = utilization;
        }
    }
}
