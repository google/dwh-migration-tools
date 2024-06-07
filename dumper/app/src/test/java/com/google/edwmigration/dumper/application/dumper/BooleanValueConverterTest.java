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
package com.google.edwmigration.dumper.application.dumper;

import static org.junit.Assert.assertEquals;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.DefaultArguments.BooleanValueConverter;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class BooleanValueConverterTest {

  @DataPoints("testCases")
  public static ImmutableList<TestCase> testCases =
      ImmutableList.of(
          TestCase.create("", false),
          TestCase.create("&*%^$*&@#*^**$", false),
          TestCase.create("true", true),
          TestCase.create("yes", true),
          TestCase.create("false", false),
          TestCase.create("1", true),
          TestCase.create("0", false));

  @Theory
  public void convert_success(@FromDataPoints("testCases") TestCase testCase) {
    assertEquals(
        testCase.convertedValue(), BooleanValueConverter.INSTANCE.convert(testCase.inputValue()));
  }

  @AutoValue
  abstract static class TestCase {
    abstract String inputValue();

    abstract boolean convertedValue();

    public static TestCase create(String inputValue, boolean convertedValue) {
      return new AutoValue_BooleanValueConverterTest_TestCase(inputValue, convertedValue);
    }
  }
}
