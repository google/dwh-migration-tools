/*
 * Copyright 2022-2023 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.connector.redshift;

import com.google.common.base.Joiner;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
/* pp */ class JdbcPropBuilder {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(JdbcPropBuilder.class);

  final String punctuations;

  final List<String> props = new ArrayList<>();

  public JdbcPropBuilder(String seps) {
    this.punctuations = seps;
  }

  @Nonnull
  public JdbcPropBuilder propOrWarn(
      @Nonnull String prop, @CheckForNull String val, @Nonnull String msg)
      throws UnsupportedEncodingException {
    if (val == null) {
      LOG.warn(msg);
    } else {
      addProp(prop, val);
    }
    return this;
  }

  @Nonnull
  public JdbcPropBuilder propOrError(
      @Nonnull String prop, @CheckForNull String val, @Nonnull String msg)
      throws MetadataDumperUsageException, UnsupportedEncodingException {
    if (val == null) {
      LOG.error(msg);
      throw new MetadataDumperUsageException(msg);
    } else {
      addProp(prop, val);
    }
    return this;
  }

  @Nonnull
  public JdbcPropBuilder prop(@Nonnull String prop, @Nonnull String val)
      throws UnsupportedEncodingException {
    if (val == null) {
      throw new InternalError("Not checked for null: " + val);
    } else {
      addProp(prop, val);
    }
    return this;
  }

  private void addProp(String prop, String val) throws UnsupportedEncodingException {
    // The encode(String, Charset) overload is JDK 10+
    props.add(prop + punctuations.charAt(1) + URLEncoder.encode(val, "UTF-8"));
  }

  @Nonnull
  public String toJdbcPart() {
    if (props.isEmpty()) return "";
    return punctuations.charAt(0) + Joiner.on(punctuations.charAt(2)).join(props);
  }
}
