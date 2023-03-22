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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility method to generate Hive partition names.
 */
public final class PartitionNameGenerator {

  private PartitionNameGenerator() {
  }

  private static final Logger LOG = LoggerFactory.getLogger(PartitionNameGenerator.class);

  /**
   * Constructs partition name from the list of partition keys and values. It:
   *
   * <ul>
   *   <li>Escapes special symbols in key and value</li>
   *   <li>Joins key and value with "="</li>
   *   <li>Joins key-value pairs with "/"</li>
   * </ul>
   *
   * Resulting partition name has a form similar to "partKey1=partValue1/partKey2=partValue2"
   *
   * <p>It copies most of the logic of the original Hive metastore function for creating partition
   * name:
   * {@link <a
   * href="https://github.com/apache/hive/blob/rel/release-2.3.6/metastore/src/java/org/apache/hadoop/hive/metastore/Warehouse.java#L315">source</a>}
   */
  public static String makePartitionName(List<String> partitionKeys, List<String> partitionValues) {
    ImmutableMap<String, String> partitionSpec = Streams.zip(partitionKeys.stream(),
            partitionValues.stream(), Maps::immutableEntry)
        .collect(toImmutableMap(Entry::getKey, Entry::getValue));

    ImmutableList<String> partitionParts =
        partitionSpec.entrySet().stream()
            .map(
                entry -> {
                  if (entry.getValue() == null || entry.getValue().length() == 0) {
                    // This is unexpected and the original code throws an exception here.
                    LOG.warn(String.format(
                        "Got empty partition value for the key %s, will ignore it.",
                        entry.getKey()));
                    return "";
                  }
                  return escapePartitionPart(entry.getKey()) + "=" + escapePartitionPart(
                      entry.getValue());
                })
            .filter(part -> !part.isEmpty())
            .collect(toImmutableList());

    return Joiner.on('/').join(partitionParts);
  }

  private static String escapePartitionPart(String partitionPart) {
    try {
      return URLEncoder.encode(partitionPart, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
}
