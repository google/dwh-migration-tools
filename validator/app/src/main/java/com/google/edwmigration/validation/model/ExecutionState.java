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
package com.google.edwmigration.validation.model;

import autovalue.shaded.com.google.errorprone.annotations.Immutable;
import com.google.common.io.Closer;
import com.google.edwmigration.validation.config.ValidationType;
import com.google.edwmigration.validation.connector.bigquery.BigQueryHandle;
import com.google.edwmigration.validation.connector.jdbc.JdbcHandle;
import java.net.URI;
import java.sql.ResultSetMetaData;
import java.util.Map;
import java.util.function.UnaryOperator;
import javax.annotation.Nullable;
import org.springframework.lang.NonNull;

@Immutable
public class ExecutionState {

  @NonNull public final UserInputContext context;
  @Nullable public final URI outputUri;
  @Nullable public final URI gcsUri;
  @Nullable public final JdbcHandle sourceHandle;
  @Nullable public final BigQueryHandle targetHandle;
  @Nullable public final ResultSetMetaData aggMetadata;
  @Nullable public final ResultSetMetaData rowMetadata;
  @Nullable public final Map<ValidationType, String> uploadedGcsUris;
  @Nullable public final Closer closer;

  private ExecutionState(Builder builder) {
    this.context = builder.context;
    this.outputUri = builder.outputUri;
    this.gcsUri = builder.gcsUri;
    this.sourceHandle = builder.sourceHandle;
    this.targetHandle = builder.targetHandle;
    this.aggMetadata = builder.aggMetadata;
    this.rowMetadata = builder.rowMetadata;
    this.uploadedGcsUris = builder.uploadedGcsUris;
    this.closer = builder.closer;
  }

  public static ExecutionState of(UserInputContext context) {
    return new Builder(context).build();
  }

  public ExecutionState map(UnaryOperator<ExecutionState.Builder> updateFn) {
    return updateFn.apply(new Builder(this)).build();
  }

  public static class Builder {
    private final UserInputContext context;
    private URI outputUri;
    private URI gcsUri;
    private JdbcHandle sourceHandle;
    private BigQueryHandle targetHandle;
    private ResultSetMetaData aggMetadata;
    private ResultSetMetaData rowMetadata;
    private Map<ValidationType, String> uploadedGcsUris;
    private Closer closer;

    // Consumed by ExecutionState.of
    private Builder(UserInputContext context) {
      this.context = context;
    }

    // Consumed by Map internally on ExecutionState instance
    private Builder(ExecutionState original) {
      this.context = original.context;
      this.outputUri = original.outputUri;
      this.gcsUri = original.gcsUri;
      this.sourceHandle = original.sourceHandle;
      this.targetHandle = original.targetHandle;
      this.aggMetadata = original.aggMetadata;
      this.rowMetadata = original.rowMetadata;
      this.uploadedGcsUris = original.uploadedGcsUris;
      this.closer = original.closer;
    }

    private ExecutionState build() {
      return new ExecutionState(this);
    }

    public Builder withOutputUri(URI outputUri) {
      this.outputUri = outputUri;
      return this;
    }

    public Builder withGcsUri(URI gcsUri) {
      this.gcsUri = gcsUri;
      return this;
    }

    public Builder withSourceHandle(JdbcHandle sourceHandle) {
      this.sourceHandle = sourceHandle;
      return this;
    }

    public Builder withTargetHandle(BigQueryHandle targetHandle) {
      this.targetHandle = targetHandle;
      return this;
    }

    public Builder withAggMetadata(ResultSetMetaData aggMetadata) {
      this.aggMetadata = aggMetadata;
      return this;
    }

    public Builder withRowMetadata(ResultSetMetaData rowMetadata) {
      this.rowMetadata = rowMetadata;
      return this;
    }

    public Builder withUploadedGcsUris(Map<ValidationType, String> uploadedGcsUris) {
      this.uploadedGcsUris = uploadedGcsUris;
      return this;
    }

    public Builder withCloser(Closer closer) {
      this.closer = closer;
      return this;
    }
  }
}
