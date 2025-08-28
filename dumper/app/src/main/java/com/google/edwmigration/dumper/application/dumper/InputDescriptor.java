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

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;

import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import java.util.ArrayList;
import java.util.Comparator;
import javax.annotation.Nonnull;

public final class InputDescriptor {
  private enum Category {
    ARGUMENT(1) {
      @Override
      String getKey(@Nonnull RespectsInput annotation) {
        return "--" + annotation.arg();
      }
    },
    ENVIRONMENT(2) {
      @Override
      String getKey(@Nonnull RespectsInput annotation) {
        return annotation.env();
      }
    },
    OTHER(3) {
      @Override
      String getKey(@Nonnull RespectsInput annotation) {
        return String.valueOf(annotation.hashCode());
      }
    };

    final int order;

    abstract String getKey(@Nonnull RespectsInput annotation);

    Category(int order) {
      this.order = order;
    }
  }

  private final RespectsInput annotation;

  public InputDescriptor(RespectsInput annotation) {
    this.annotation = annotation;
  }

  @Nonnull
  private Category getCategory() {
    if (!isNullOrEmpty(annotation.arg())) {
      return Category.ARGUMENT;
    }
    if (!isNullOrEmpty(annotation.env())) {
      return Category.ENVIRONMENT;
    }
    return Category.OTHER;
  }

  public static Comparator<InputDescriptor> comparator() {
    return comparing(InputDescriptor::categoryOrder)
        .thenComparing(InputDescriptor::annotationOrder);
  }

  @Override
  public String toString() {
    ArrayList<String> buf = new ArrayList<>();
    buf.add(String.format("%-12s", getKey()));
    if (getCategory() == Category.ENVIRONMENT) {
      buf.add("(environment variable)");
    }
    buf.add(defaultHint(annotation.defaultValue()));
    buf.add(annotation.description());
    buf.add(requiredHint(annotation.required()));

    return buf.stream().filter(el -> !el.isEmpty()).collect(joining(" "));
  }

  public String getKey() {
    return getCategory().getKey(annotation);
  }

  private String defaultHint(String value) {
    if (isNullOrEmpty(value)) {
      return "";
    } else {
      return String.format("(default: %s)", value);
    }
  }

  private String requiredHint(String value) {
    if (isNullOrEmpty(value)) {
      return "";
    } else {
      return String.format("(Required %s.)", value);
    }
  }

  private int annotationOrder() {
    return annotation.order();
  }

  private int categoryOrder() {
    return getCategory().order;
  }
}
