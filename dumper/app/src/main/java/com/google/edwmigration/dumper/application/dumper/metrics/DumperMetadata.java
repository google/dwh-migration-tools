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
package com.google.edwmigration.dumper.application.dumper.metrics;

public class DumperMetadata {

  private final String version;
  private final String gitCommit;
  private final String buildDate;

  public DumperMetadata(String version, String gitCommit, String buildDate) {
    this.version = version;
    this.gitCommit = gitCommit;
    this.buildDate = buildDate;
  }

  public String getVersion() {
    return version;
  }

  public String getGitCommit() {
    return gitCommit;
  }

  public final String getBuildDate() {
    return buildDate;
  }
}
