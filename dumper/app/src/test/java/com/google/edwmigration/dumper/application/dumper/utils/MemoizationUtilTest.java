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
package com.google.edwmigration.dumper.application.dumper.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.commons.io.function.IOFunction;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class MemoizationUtilTest {

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock private IOFunction<String, Integer> delegateIoFunction;

  @Test
  public void memoization_success() throws Exception {
    when(delegateIoFunction.apply("input")).thenReturn(42);
    IOFunction<String, Integer> underTest = MemoizationUtil.createMemoizer(delegateIoFunction);

    // Act
    Integer result1 = underTest.apply("input");
    Integer result2 = underTest.apply("input");

    // Verify
    assertEquals(Integer.valueOf(42), result1);
    assertEquals(Integer.valueOf(42), result2);
    verify(delegateIoFunction).apply("input");
    verifyNoMoreInteractions(delegateIoFunction);
  }

  @Test
  public void memoization_ioExceptionGetsPropagated() throws Exception {
    when(delegateIoFunction.apply("input")).thenThrow(new IOException("test IO error"));
    IOFunction<String, Integer> underTest = MemoizationUtil.createMemoizer(delegateIoFunction);

    // Act
    IOException exception = assertThrows(IOException.class, () -> underTest.apply("input"));

    // Verify
    assertEquals(exception.getMessage(), "test IO error");
  }

  @Test
  public void memoization_runtimeExceptionGetsPropagated() throws Exception {
    when(delegateIoFunction.apply("input"))
        .thenThrow(new IllegalStateException("test illegal state error"));
    IOFunction<String, Integer> underTest = MemoizationUtil.createMemoizer(delegateIoFunction);

    // Act
    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> underTest.apply("input"));

    // Verify
    assertEquals(exception.getMessage(), "test illegal state error");
    assertEquals(exception.getClass(), IllegalStateException.class);
  }
}
