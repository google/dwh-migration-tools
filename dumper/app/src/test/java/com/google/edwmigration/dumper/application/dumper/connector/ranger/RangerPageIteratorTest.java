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
package com.google.edwmigration.dumper.application.dumper.connector.ranger;

import static java.util.Collections.emptyList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerPageIterator.Page;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import junit.framework.TestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class RangerPageIteratorTest extends TestCase {

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock private Function<Page, List<Integer>> apiMock;

  @Test
  public void next_worksWithSinglePage() {
    when(apiMock.apply(Page.create(0, 100))).thenReturn(Arrays.asList(0, 1, 2));

    List<Integer> actualList = new ArrayList<>();
    new RangerPageIterator<>(apiMock, 100).forEachRemaining(actualList::add);

    assertEquals(Arrays.asList(0, 1, 2), actualList);
    verify(apiMock, times(1)).apply(any(Page.class));
  }

  @Test
  public void next_worksWithMultiplePagesLastPageNotEmpty() {
    when(apiMock.apply(Page.create(0, 3))).thenReturn(Arrays.asList(0, 1, 2));
    when(apiMock.apply(Page.create(3, 3))).thenReturn(Arrays.asList(3, 4));

    List<Integer> actualList = new ArrayList<>();
    new RangerPageIterator<>(apiMock, 3).forEachRemaining(actualList::add);

    assertEquals(Arrays.asList(0, 1, 2, 3, 4), actualList);
    verify(apiMock, times(2)).apply(any(Page.class));
  }

  @Test
  public void next_worksWithMultiplePagesLastPageEmpty() {
    when(apiMock.apply(Page.create(0, 3))).thenReturn(Arrays.asList(0, 1, 2));
    when(apiMock.apply(Page.create(3, 3))).thenReturn(Arrays.asList(3, 4, 5));
    when(apiMock.apply(Page.create(6, 3))).thenReturn(emptyList());

    List<Integer> actualList = new ArrayList<>();
    new RangerPageIterator<>(apiMock, 3).forEachRemaining(actualList::add);

    assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5), actualList);
    verify(apiMock, times(3)).apply(any(Page.class));
  }
}
