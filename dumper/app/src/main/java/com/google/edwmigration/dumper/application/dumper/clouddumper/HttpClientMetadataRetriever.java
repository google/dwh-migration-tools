/*
 * Copyright 2022-2024 Google LLC
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
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.http.HttpStatus;

public class HttpClientMetadataRetriever implements MetadataRetriever {

  private static final String METADATA_BASE_URL =
      "http://metadata.google.internal/computeMetadata/v1/instance/";

  private final CloseableHttpClient httpClient;

  public HttpClientMetadataRetriever(CloseableHttpClient httpClient) {
    this.httpClient = httpClient;
  }

  @Override
  public Optional<String> get(String key) throws IOException, HttpException {
    ClassicHttpRequest httpGet =
        ClassicRequestBuilder.get(METADATA_BASE_URL + key)
            .setHeader("Metadata-Flavor", "Google")
            .build();
    return httpClient.<Optional<String>>execute(
        httpGet,
        response -> {
          int statusCode = response.getCode();
          if (statusCode == HttpStatus.SC_NOT_FOUND) {
            return Optional.<String>empty();
          }
          if (statusCode == HttpStatus.SC_OK) {
            return Optional.of(EntityUtils.toString(response.getEntity()));
          }
          throw new HttpException(String.format("Got unexpected status code %d.", statusCode));
        });
  }
}
