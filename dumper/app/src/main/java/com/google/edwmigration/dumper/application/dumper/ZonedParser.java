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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import joptsimple.ValueConversionException;
import joptsimple.ValueConverter;

public class ZonedParser implements ValueConverter<ZonedDateTime> {

  public static final String DEFAULT_PATTERN = "yyyy-MM-dd[ HH:mm:ss[.SSS]]";
  private final DayOffset dayOffset;
  private final DateTimeFormatter parser;

  public static ZonedParser withDefaultPattern(DayOffset dayOffset) {
    return new ZonedParser(DEFAULT_PATTERN, dayOffset);
  }

  private ZonedParser(String pattern, DayOffset dayOffset) {
    this.dayOffset = dayOffset;
    this.parser =
        DateTimeFormatter.ofPattern(pattern, Locale.US).withResolverStyle(ResolverStyle.LENIENT);
  }

  @Override
  public ZonedDateTime convert(String value) {

    TemporalAccessor result = parser.parseBest(value, LocalDateTime::from, LocalDate::from);

    if (result instanceof LocalDateTime) {
      return ((LocalDateTime) result).atZone(ZoneOffset.UTC);
    }

    if (result instanceof LocalDate) {
      return ((LocalDate) result)
          .plusDays(dayOffset.getValue())
          .atTime(LocalTime.MIDNIGHT)
          .atZone(ZoneOffset.UTC);
    }

    throw new ValueConversionException("Value " + value + " cannot be parsed to date or datetime");
  }

  @Override
  public Class<ZonedDateTime> valueType() {
    return ZonedDateTime.class;
  }

  @Override
  public String valuePattern() {
    return null;
  }

  public enum DayOffset {
    START_OF_DAY(0L),
    END_OF_DAY(1L);

    private final long value;

    DayOffset(long value) {
      this.value = value;
    }

    public long getValue() {
      return value;
    }
  }
}
