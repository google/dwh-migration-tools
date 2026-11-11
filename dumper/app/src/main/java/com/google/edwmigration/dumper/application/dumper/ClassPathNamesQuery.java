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
package com.google.edwmigration.dumper.application.dumper;

import com.google.common.base.Splitter;
import java.io.File;
import java.util.LinkedHashMap;
import org.anarres.jdiagnostics.AbstractQuery;
import org.anarres.jdiagnostics.Result;

/**
 * Reports the file names of the classpath entries, in classpath order.
 *
 * <p>The names are enough to spot an unexpected or duplicate JAR. Directories are left out because
 * the report is written into the output archive, and the files are not read because the report is
 * built for every failed task.
 */
class ClassPathNamesQuery extends AbstractQuery {

  private static final String CLASS_PATH_PROPERTY = "java.class.path";

  @Override
  public String getName() {
    return "classpath";
  }

  @Override
  protected Result newResult() {
    return new Result(new LinkedHashMap<String, Object>());
  }

  @Override
  public void call(Result result, String prefix) {
    String classPath = System.getProperty(CLASS_PATH_PROPERTY);
    if (classPath == null) {
      return;
    }

    int index = 0;
    for (String entry : Splitter.on(File.pathSeparatorChar).omitEmptyStrings().split(classPath)) {
      result.put(prefix + index, new File(entry).getName());
      index++;
    }
  }
}
