package com.google.edwmigration.dbsync.common;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import org.junit.jupiter.api.Test;

class UriUtilTest {

  @Test
  void ensureTrailingSlash_addMissingSlash() {
    assertEquals("gs://path/to/folder/", UriUtil.ensureTrailingSlash("gs://path/to/folder"));
    assertEquals("/path/to/folder/", UriUtil.ensureTrailingSlash("/path/to/folder"));
    assertEquals("path/to/folder/", UriUtil.ensureTrailingSlash("path/to/folder"));
  }

  @Test
  void ensureTrailingSlash_retainsExistingSlash() {
    assertEquals("gs://path/to/folder/", UriUtil.ensureTrailingSlash("gs://path/to/folder/"));
    assertEquals("/path/to/folder/", UriUtil.ensureTrailingSlash("/path/to/folder/"));
    assertEquals("path/to/folder/", UriUtil.ensureTrailingSlash("path/to/folder/"));
  }

  @Test
  void getRelativePath_extractFilePath() {
    assertEquals(
        "path/to/file.txt", UriUtil.getRelativePath(URI.create("gs://bucket/path/to/file.txt")));
  }

  @Test
  void getRelativePath_extractFolderPath() {
    assertEquals("path/to/file/", UriUtil.getRelativePath(URI.create("gs://bucket/path/to/file/")));
  }

  @Test
  void getRelativePath_returnEmptyStringWhenNoPathPresent() {
    assertEquals("", UriUtil.getRelativePath(URI.create("gs://bucket/")));
  }
}
