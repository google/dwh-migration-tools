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
package com.google.edwmigration.dumper.application.dumper;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractJdbcConnector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DriverClasspathTest {

  private static final Path RESOURCES =
      Paths.get(Resources.getResource("driver-classpath-test/").getPath());

  private static Path supportJarPath;
  private static Path driverJarPath;

  @BeforeClass
  public static void setUp() throws IOException {
    supportJarPath = buildJarFile("foo/baz/Support", "support.jar", null);
    driverJarPath = buildJarFile("foo/bar/DummyDriver", "driver.jar", supportJarPath.toString());
  }

  private static Path buildJarFile(String className, String jarName, String classPath)
      throws IOException {
    Path javaFilePath = RESOURCES.resolve(className + ".java");
    compileJavaFile(javaFilePath, classPath);
    return compressJarFile(className, jarName);
  }

  private static Path compressJarFile(String className, String jarName) throws IOException {
    Path jarFilePath = RESOURCES.resolve(jarName);
    try (JarOutputStream jarOutputStream =
        new JarOutputStream(Files.newOutputStream(jarFilePath), new Manifest())) {
      String classFileName = className + ".class";
      jarOutputStream.putNextEntry(new JarEntry(classFileName));
      Files.copy(RESOURCES.resolve(classFileName), jarOutputStream);
      return jarFilePath;
    }
  }

  private static void compileJavaFile(@Nonnull Path javaFilePath, @CheckForNull String classpath) {
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

    List<String> options = classpath == null ? null : Lists.newArrayList("-classpath", classpath);

    Iterable<? extends JavaFileObject> files =
        compiler.getStandardFileManager(null, null, null).getJavaFileObjects(javaFilePath.toFile());

    CompilationTask task = compiler.getTask(null, null, null, options, null, files);
    task.call();
  }

  @Test
  public void whenDriverAndSupport_noExceptionThrown() throws Exception {

    String driverPaths = driverJarPath + "," + supportJarPath;
    String[] args = {"--connector", "ignored", "--driver", driverPaths};

    try (Handle ignored = new DummyConnector("dummy").open(new ConnectorArguments(args))) {
      Assert.assertTrue("Driver instantiated.", true);
    }
  }

  @Test
  public void whenDriverOnly_exceptionThrown() {

    String driverPaths = driverJarPath.toString();
    String[] args = {"--connector", "ignored", "--driver", driverPaths};

    SQLException exception =
        Assert.assertThrows(
            SQLException.class,
            () -> {
              try (Handle ignored =
                  new DummyConnector("dummy").open(new ConnectorArguments(args))) {
                Assert.assertTrue("Driver instantiated.", true);
              }
            });

    Assert.assertTrue(exception.getCause() instanceof InvocationTargetException);
    Throwable targetException =
        ((InvocationTargetException) exception.getCause()).getTargetException();
    Assert.assertTrue(targetException instanceof NoClassDefFoundError);
    Assert.assertEquals("foo/baz/Support", targetException.getMessage());
  }

  private static class DummyConnector extends AbstractJdbcConnector {

    public DummyConnector(@Nonnull String name) {
      super(name);
    }

    @Nonnull
    @Override
    public String getDefaultFileName(boolean ignored) {
      return StringUtils.EMPTY;
    }

    @Override
    public void addTasksTo(
        @Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments) {}

    @Nonnull
    @Override
    public Handle open(@Nonnull ConnectorArguments arguments) throws Exception {
      newDriver(arguments.getDriverPaths(), "foo.bar.DummyDriver");
      return () -> {};
    }
  }
}
