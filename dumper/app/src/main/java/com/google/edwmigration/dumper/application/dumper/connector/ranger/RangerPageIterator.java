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

import com.google.auto.value.AutoValue;
import com.google.common.collect.AbstractIterator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import javax.annotation.CheckForNull;

/**
 * An iterator that iterates sequentially each item through all the pages of a paginated Ranger API
 * response.
 */
class RangerPageIterator<T> extends AbstractIterator<T> {

  @AutoValue
  public abstract static class Page {
    static Page create(int offset, int limit) {
      return new AutoValue_RangerPageIterator_Page(offset, limit);
    }

    abstract int offset();

    abstract int limit();
  }

  private final Function<Page, List<T>> fetcher;
  private final int pageSize;
  private int offset;
  private Iterator<T> pageIterator;
  private boolean lastPage = false;

  RangerPageIterator(Function<Page, List<T>> fetcher, int pageSize) {
    this.fetcher = fetcher;
    this.pageSize = pageSize;
    offset = 0;
  }

  @CheckForNull
  @Override
  protected T computeNext() {
    if (pageIterator == null || !pageIterator.hasNext() && !lastPage) {
      List<T> entries = fetcher.apply(Page.create(offset, pageSize));
      offset += pageSize;
      lastPage = entries.size() < pageSize;
      pageIterator = entries.iterator();
    }
    if (!pageIterator.hasNext()) {
      return endOfData();
    }
    return pageIterator.next();
  }
}
