package com.google.edwmigration.dumper.application.dumper.connector.cloudera;

import com.cloudera.api.swagger.ClustersResourceApi;
import com.cloudera.api.swagger.client.ApiException;
import com.cloudera.api.swagger.model.ApiClusterList;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;

import javax.annotation.Nonnull;

public abstract class AbstractClouderaTask extends AbstractTask<Void> {

    public AbstractClouderaTask(String targetPath) {
        super(targetPath);
    }

    @Nonnull
    protected ApiClusterList getClusters(@Nonnull ClouderaHandle handle) throws ApiException {
        ClustersResourceApi api = new ClustersResourceApi(handle.getClient());
        return api.readClusters(null, null);
    }
}
