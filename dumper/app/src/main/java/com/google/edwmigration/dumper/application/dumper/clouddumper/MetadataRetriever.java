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

import java.io.IOException;
import java.util.Optional;
import org.apache.hc.core5.http.HttpException;

/** Retriever of metadata information. */
public interface MetadataRetriever {

  /** Gets the metadata for the given key. */
  Optional<String> get(String key) throws IOException, HttpException;

  /** Gets the attribute for the given key. */
  default Optional<String> getAttribute(String key) throws IOException, HttpException {
    return get("attributes/" + key);
  }
}
