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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

abstract class AbstractClouderaManagerTask extends AbstractTask<Void> {
  private final ObjectMapper objectMapper =
      new ObjectMapper()
          .configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, true)
          .configure(DeserializationFeature.FAIL_ON_TRAILING_TOKENS, true);

  public AbstractClouderaManagerTask(String targetPath) {
    super(targetPath);
  }

  @CheckForNull
  @Override
  protected final Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    doRun(context, sink, (ClouderaManagerHandle) handle);
    return null;
  }

  protected final boolean isStatusCodeOK(int statusCode) {
    // Based on HTTP rfc: https://datatracker.ietf.org/doc/html/rfc7231#section-6.3
    return 200 <= statusCode && statusCode <= 299;
  }

  protected abstract void doRun(
      TaskRunContext context, @Nonnull ByteSink sink, @Nonnull ClouderaManagerHandle handle)
      throws Exception;

  protected JsonNode readJsonTree(InputStream inputStream) throws IOException {
    return objectMapper.readTree(inputStream);
  }

  protected <T> T parseJsonStringToObject(String jsonString, Class<T> type)
      throws JsonProcessingException {
    return objectMapper.readValue(jsonString, type);
  }

  protected <T> T parseJsonStreamToObject(InputStream src, Class<T> type) throws IOException {
    return objectMapper.readValue(src, type);
  }

  protected String parseObjectToJsonString(Object obj) throws JsonProcessingException {
    return objectMapper.writeValueAsString(obj);
  }
}
