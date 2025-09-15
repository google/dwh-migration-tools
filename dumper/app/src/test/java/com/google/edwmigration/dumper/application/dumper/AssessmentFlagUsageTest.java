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

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableCollection;
import com.google.edwmigration.dumper.application.dumper.annotations.AvoidArgumentAssessment;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentAssessment;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import java.lang.annotation.Annotation;
import org.junit.Test;
import org.springframework.core.annotation.AnnotationUtils;

public class AssessmentFlagUsageTest {

  @Test
  public void allTheConnectorsExplicitlyChoseAssessmentFlag() {
    ImmutableCollection<Connector> connectors = allConnectors();
    for (Connector connector : connectors) {
      RespectsArgumentAssessment respect =
          findAnnotation(connector, RespectsArgumentAssessment.class);
      AvoidArgumentAssessment avoid = findAnnotation(connector, AvoidArgumentAssessment.class);

      String assertMessage =
          "The connector "
              + connector
              + " must use one of "
              + RespectsArgumentAssessment.class.getSimpleName()
              + " or "
              + AvoidArgumentAssessment.class.getSimpleName();
      assertTrue(assertMessage, respect != null ^ avoid != null);
    }
  }

  @Test
  public void assessmentFlagIsExplicitlySpecifiedAndUsed() throws Exception {
    ImmutableCollection<Connector> connectors = allConnectors();
    for (Connector connector : connectors) {
      RespectsArgumentAssessment assessment =
          findAnnotation(connector, RespectsArgumentAssessment.class);
      if (assessment == null) {
        continue;
      }

      assertThrows(Exception.class, () -> connector.validate(new ConnectorArguments()));
    }
  }

  private static <A extends Annotation> A findAnnotation(
      Connector connector, Class<A> annotationType) {
    return AnnotationUtils.findAnnotation(connector.getClass(), annotationType);
  }

  private static ImmutableCollection<Connector> allConnectors() {
    return ConnectorRepository.getInstance().getAllConnectors();
  }
}
