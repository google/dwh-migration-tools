/*
 * Copyright 2022-2023 Google LLC
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
package com.google.edwmigration.dumper.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

  @Nonnull private static final String TEST_OUTPUTS_DIR = "build" + File.separator + "test-outputs";

  /** Returns a filename in build/test-outputs, ensuring that the parent directory exists. */
  @Nonnull
  public static File newOutputFileSilent(@Nonnull String name) throws IOException {
    File file = new File(TEST_OUTPUTS_DIR, name);
    File parent = file.getParentFile();
    if (parent != null) FileUtils.forceMkdir(parent);
    return file;
  }

  /** Returns a filename in build/test-outputs, ensuring that the parent directory exists. */
  @Nonnull
  public static File newOutputFile(@Nonnull String name) throws IOException {
    File file = newOutputFileSilent(name);
    LOG.info("Creating test output file " + file.getAbsolutePath());
    return file;
  }

  /** Mostly for static or variable initializers. */
  @Nonnull
  public static File newOutputFileUnchecked(@Nonnull String name) throws RuntimeException {
    try {
      return newOutputFileSilent(name);
    } catch (IOException e) {
      throw new AssertionError("Failed to create output file " + name, e);
    }
  }

  @Nonnull
  public static File newOutputDirectory(@Nonnull String name) throws IOException {
    File file = newOutputFile(name);
    try {
      FileUtils.forceDelete(file);
    } catch (FileNotFoundException fnfe) {
      // Ignore; the directory doesn't exist, but we're going to create it next.
    }
    FileUtils.forceMkdir(file);
    return file;
  }

  /** Mostly for static or variable initializers. */
  @Nonnull
  public static File newOutputDirectoryUnchecked(@Nonnull String name) throws RuntimeException {
    try {
      return newOutputDirectory(name);
    } catch (IOException e) {
      throw new AssertionError("Failed to create output directory " + name, e);
    }
  }

  @Nonnull
  public static File getProjectRootDir() {
    File cwd = SystemUtils.getUserDir().getAbsoluteFile();
    File dir = cwd;
    while (dir != null) {
      File file = new File(dir, "settings.gradle");
      if (file.exists()) {
        LOG.debug("Project root dir is " + dir);
        return dir;
      }
      dir = dir.getParentFile();
    }
    throw new IllegalStateException("Cannot find root dir starting from " + cwd);
  }

  @Nonnull
  public static File getTestResourcesDir(String subproject) {
    File moduleDir = new File(getProjectRootDir(), subproject);
    File resourcesDir = new File(moduleDir, "build/resources/test");
    if (resourcesDir == null) {
      throw new IllegalStateException("Cannot find resources dir " + resourcesDir);
    }
    return resourcesDir;
  }
}
