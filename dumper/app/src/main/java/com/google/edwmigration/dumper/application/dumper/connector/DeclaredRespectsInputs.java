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

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Finds the {@link RespectsInput} annotations declared on a class: the ones written on the class
 * itself, alone or repeated, and the ones carried by the other annotations on the class, such as
 * {@code @RespectsArgumentUser}. Annotations of superclasses are not included.
 */
final class DeclaredRespectsInputs {

  private DeclaredRespectsInputs() {}

  /**
   * Returns the annotations nearest to the class first: the ones on the class itself, then the ones
   * on its annotations, then the ones on the annotations of those, each in declaration order.
   */
  @Nonnull
  static ImmutableList<RespectsInput> find(@Nonnull Class<?> type) {
    ImmutableList.Builder<RespectsInput> result = ImmutableList.builder();
    Set<Class<? extends Annotation>> visited = new HashSet<>();
    List<AnnotatedElement> elements = ImmutableList.of(type);
    while (!elements.isEmpty()) {
      List<AnnotatedElement> annotationTypes = new ArrayList<>();
      for (AnnotatedElement element : elements) {
        result.add(element.getDeclaredAnnotationsByType(RespectsInput.class));
        for (Annotation annotation : element.getDeclaredAnnotations()) {
          // Annotations such as @Documented are annotated with themselves.
          if (visited.add(annotation.annotationType())) {
            annotationTypes.add(annotation.annotationType());
          }
        }
      }

      elements = annotationTypes;
    }

    return result.build();
  }
}
