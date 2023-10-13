package com.google.edwmigration.dumper.application.dumper.connector.cloudera;

import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;

public abstract class AbstractClouderaTask extends AbstractTask<Void> {

  public AbstractClouderaTask(String targetPath) {
    super(targetPath);
  }
}
