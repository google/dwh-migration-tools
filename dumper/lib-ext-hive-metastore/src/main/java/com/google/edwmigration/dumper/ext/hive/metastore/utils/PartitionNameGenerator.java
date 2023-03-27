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
package com.google.edwmigration.dumper.ext.hive.metastore.utils;

import com.google.common.collect.Streams;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.CheckForNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility method to generate Hive partition names. */
public final class PartitionNameGenerator {

  private PartitionNameGenerator() {}

  private static final Logger LOG = LoggerFactory.getLogger(PartitionNameGenerator.class);

  /**
   * Constructs partition name from the list of partition keys and values. It:
   *
   * <ul>
   *   <li>Escapes special symbols in key and value
   *   <li>Joins key and value with "="
   *   <li>Joins key-value pairs with "/"
   * </ul>
   *
   * Resulting partition name has a form similar to "partKey1=partValue1/partKey2=partValue2"
   *
   * <p>It copies most of the logic of the original Hive metastore function for creating partition
   * name: {@link <a
   * href="https://github.com/apache/hive/blob/rel/release-2.3.6/metastore/src/java/org/apache/hadoop/hive/metastore/Warehouse.java#L315">source</a>}
   */
  public static String makePartitionName(List<String> partitionKeys, List<String> partitionValues) {
    return Streams.zip(
            partitionKeys.stream(),
            partitionValues.stream(),
            PartitionNameGenerator::constructPartitionName)
        .filter(Objects::nonNull)
        .collect(Collectors.joining("/"));
  }

  @CheckForNull
  private static String constructPartitionName(String partitionKey, String partitionValue) {
    if (partitionValue == null || partitionValue.length() == 0) {
      // This is unexpected and the original code throws an exception here.
      LOG.warn(
          String.format("Got empty partition value for the key %s, will ignore it.", partitionKey));
      return null;
    }
    return escapePartitionPart(partitionKey) + "=" + escapePartitionPart(partitionValue);
  }

  private static String escapePartitionPart(String partitionPart) {
    try {
      return URLEncoder.encode(partitionPart, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
}
