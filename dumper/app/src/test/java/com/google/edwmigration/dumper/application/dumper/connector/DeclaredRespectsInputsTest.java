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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDriverRequired;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentUser;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentsStartEndDate;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInputs;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DeclaredRespectsInputsTest {

  @Test
  public void find_noAnnotations_empty() {
    assertEquals(ImmutableList.of(), DeclaredRespectsInputs.find(NotAnnotated.class));
  }

  @Test
  public void find_singleAnnotation_found() {
    assertEquals(ImmutableList.of("single"), findArgs(Single.class));
  }

  @Test
  public void find_repeatedAnnotation_allFoundInOrder() {
    assertEquals(ImmutableList.of("first", "second"), findArgs(Repeated.class));
  }

  @Test
  public void find_containerAnnotation_allFoundInOrder() {
    assertEquals(ImmutableList.of("first", "second"), findArgs(Container.class));
  }

  @Test
  public void find_composedAnnotation_found() {
    assertEquals(ImmutableList.of(ConnectorArguments.OPT_USER), findArgs(Composed.class));
  }

  @Test
  public void find_composedAnnotationWithRepeatedInputs_allFoundInOrder() {
    assertEquals(
        ImmutableList.of(ConnectorArguments.OPT_START_DATE, ConnectorArguments.OPT_END_DATE),
        findArgs(ComposedRepeated.class));
  }

  @Test
  public void find_directAndComposed_directFirst() {
    assertEquals(
        ImmutableList.of("direct", ConnectorArguments.OPT_DRIVER, ConnectorArguments.OPT_USER),
        findArgs(DirectAndComposed.class));
  }

  @Test
  public void find_subclass_superclassAnnotationsNotIncluded() {
    assertEquals(ImmutableList.of("sub"), findArgs(Sub.class));
  }

  private static List<String> findArgs(Class<?> type) {
    List<String> args = new ArrayList<>();
    for (RespectsInput input : DeclaredRespectsInputs.find(type)) {
      args.add(input.arg());
    }

    return args;
  }

  private static class NotAnnotated {}

  @RespectsInput(arg = "single", description = "")
  private static class Single {}

  @RespectsInput(arg = "first", description = "")
  @RespectsInput(arg = "second", description = "")
  private static class Repeated {}

  @RespectsInputs({
    @RespectsInput(arg = "first", description = ""),
    @RespectsInput(arg = "second", description = "")
  })
  private static class Container {}

  @RespectsArgumentUser
  private static class Composed {}

  @RespectsArgumentsStartEndDate
  private static class ComposedRepeated {}

  @RespectsArgumentDriverRequired
  @RespectsInput(arg = "direct", description = "")
  @RespectsArgumentUser
  private static class DirectAndComposed {}

  @RespectsInput(arg = "sub", description = "")
  private static class Sub extends Single {}
}
