package com.google.edwmigration.dbsync.common;

import java.net.URI;

public class UriUtil {
  public static String ensureTrailingSlash(String uri) {
    if (uri.endsWith("/")) {
      return uri;
    }
    return uri + "/";
  }

  public static String getRelativePath(URI uri) {
    String path = uri.getPath();
    if (path.startsWith("/")) {
      return path.substring(1);
    }
    return path;
  }
}
