package com.google.edwmigration.dbsync.storage.aws;

import java.io.FileNotFoundException;

/* pp */ class S3Utils {
  public static FileNotFoundException newFileNotFoundException(String message, Throwable cause) {
    FileNotFoundException e = new FileNotFoundException(message);
    e.initCause(cause);
    return e;
  }
}
