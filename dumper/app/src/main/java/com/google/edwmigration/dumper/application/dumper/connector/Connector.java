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
package com.google.edwmigration.dumper.application.dumper.connector;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.springframework.core.annotation.AnnotationUtils.getDeclaredRepeatableAnnotations;

import autovalue.shaded.com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.InputDescriptor;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.io.IOException;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

/** @author shevek */
public interface Connector {

  // Empty
  enum DefaultProperties implements ConnectorProperty {}

  @Nonnull
  default String getDescription() {
    return "";
  }

  @Nonnull
  String getName();

  @Nonnull
  String getDefaultFileName(boolean isAssessment, Clock clock);

  /**
   * Validates if the cli parameters passed to the particular connector are expected. The method is
   * called before {@link Connector#open(ConnectorArguments)} and {@link Connector#addTasksTo(List,
   * ConnectorArguments)}
   *
   * @param arguments cli params
   * @throws RuntimeException if incorrect set of arguments passed to the particular connector
   */
  default void validate(@Nonnull ConnectorArguments arguments) {}

  static void validateDateRange(@Nonnull ConnectorArguments arguments) {
    ZonedDateTime start = arguments.getStartDate();
    ZonedDateTime end = arguments.getEndDate();

    if (start == null && end == null) {
      return;
    }
    checkNotNull(start, "End date can be specified only with start date, but start date was null.");
    // The assignment makes 'end' recognized as @Nonnull.
    end = checkNotNull(end, "End date must be specified with start date, but was null.");

    String message = String.format("Start date [%s] must be before end date [%s].", start, end);
    checkState(end.isAfter(start), message);
  }

  void addTasksTo(@Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws Exception;

  @Nonnull
  Handle open(@Nonnull ConnectorArguments arguments) throws Exception;

  @Nonnull
  Iterable<ConnectorProperty> getPropertyConstants();

  default void printHelp(@Nonnull Appendable out) throws IOException {
    out.append("* " + getName());
    String description = getDescription();
    if (!description.isEmpty()) {
      out.append(" - " + description);
    }
    out.append("\n");
    for (InputDescriptor descriptor : getAcceptsInputs(this)) {
      out.append(String.format("%8s%s\n", "", descriptor));
    }
  }

  @Nonnull
  static Collection<InputDescriptor> getAcceptsInputs(@Nonnull Connector connector) {

    ArrayList<Class<?>> classes = new ArrayList<>();
    for (Class<?> type = connector.getClass(); type != null; type = type.getSuperclass()) {
      classes.add(type);
    }

    Map<String, InputDescriptor> map = new HashMap<>();
    for (Class<?> type : classes) {
      Set<RespectsInput> respectsInputs =
          getDeclaredRepeatableAnnotations(type, RespectsInput.class);
      for (RespectsInput item : respectsInputs) {
        InputDescriptor descriptor = new InputDescriptor(item);
        map.putIfAbsent(descriptor.getKey(), descriptor);
      }
    }

    return map.values().stream()
        .sorted(InputDescriptor.comparator())
        .collect(ImmutableList.toImmutableList());
  }
}
