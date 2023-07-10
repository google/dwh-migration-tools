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
package com.google.edwmigration.dumper.application.dumper.clouddumper;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.util.Optional;

@AutoValue
public abstract class DriverInformation {

  public static DriverInformation.Builder builder(String name, URI uri) {
    return new AutoValue_DriverInformation.Builder().setName(name).setUri(uri).setAliases();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setName(String name);

    public abstract Builder setAliases(String... aliases);

    public abstract Builder setUri(URI uri);

    public abstract Builder setChecksum(byte[] checksum);

    public abstract DriverInformation build();
  }

  public abstract String name();

  public abstract ImmutableList<String> aliases();

  public abstract URI uri();

  public abstract Optional<byte[]> checksum();

  public String getDriverFileName() {
    String path = uri().getPath();
    return path.substring(path.lastIndexOf('/') + 1);
  }
}
