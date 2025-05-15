/*
 * Copyright 2022-2025 Google LLC
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
package com.google.edwmigration.permissions.files;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.permissions.ProcessingException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recognizes if given path is a GCS resource or a local file, creates a Path connected to a
 * relevant file system and invokes the callback for further processing.
 */
public class FileProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(FileProcessor.class);

  private static ImmutableList<PathResolver> pathResolvers =
      Stream.of(new GcsPathResolver(), new LocalPathResolver())
          .map(ZipPathResolverDecorator::new)
          .collect(toImmutableList());

  private FileProcessor() {} // Prevent instantiation

  @FunctionalInterface
  public static interface ThrowingFunction<T, R> {
    R apply(T t) throws IOException;
  }

  @FunctionalInterface
  public static interface ThrowingConsumer<T> {
    void accept(T t) throws IOException;
  }

  /**
   * Applies a function to a path, handling GCS and local files.
   *
   * @param filePath The path to the file.
   * @param process The function to apply to the path.
   * @param <T> The return type of the function.
   * @return The result of the function.
   */
  public static <T> T apply(String filePath, ThrowingFunction<Path, T> process) {
    return applyFunction(
        filePath,
        path -> {
          try {
            return process.apply(path);
          } catch (IOException e) {
            throw new ProcessingException(
                String.format("Error processing file: '%s'", filePath), e);
          }
        });
  }

  /**
   * Applies a consumer to a path, handling GCS and local files.
   *
   * @param filePath The path to the file.
   * @param process The consumer to apply to the path.
   */
  public static void applyConsumer(String filePath, ThrowingConsumer<Path> process) {
    apply(
        filePath,
        path -> {
          process.accept(path);
          return (Void) null;
        });
  }

  private static <T> T applyFunction(String filePath, Function<Path, T> process) {
    PathResolver pathResolver =
        pathResolvers.stream()
            .filter(pathProcessor -> pathProcessor.canSupport(filePath))
            .findFirst()
            .orElseThrow(
                () ->
                    new ProcessingException(
                        String.format("No PathResolver match for file: '%s'", filePath)));
    return pathResolver.apply(filePath, process);
  }
}
