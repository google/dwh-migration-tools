package com.google.edwmigration.dumper.application.dumper.connector.databricks;

import com.google.edwmigration.dumper.application.dumper.handle.AbstractHandle;
import java.io.IOException;

public class DatabricksHandle extends AbstractHandle {
  private final DatabricksClient client;

  public DatabricksHandle(DatabricksClient client) {
    this.client = client;
  }

  public DatabricksClient getClient() {
    return client;
  }

  @Override
  public void close() throws IOException {
    // nothing to close for this simple prototype
  }
}
