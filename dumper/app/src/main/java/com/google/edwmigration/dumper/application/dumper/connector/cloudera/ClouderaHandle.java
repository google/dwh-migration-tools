package com.google.edwmigration.dumper.application.dumper.connector.cloudera;

import com.cloudera.api.swagger.client.ApiClient;
import com.google.edwmigration.dumper.application.dumper.handle.AbstractHandle;

public class ClouderaHandle extends AbstractHandle {

    private final ApiClient client;

    public ClouderaHandle(ApiClient client) {
        this.client = client;
    }

    public ApiClient getClient() {
        return client;
    }
}