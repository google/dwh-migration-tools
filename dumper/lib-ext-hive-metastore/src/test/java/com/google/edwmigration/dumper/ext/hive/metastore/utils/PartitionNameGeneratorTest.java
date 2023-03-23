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

import static com.google.edwmigration.dumper.ext.hive.metastore.utils.PartitionNameGenerator.makePartitionName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PartitionNameGeneratorTest {
  @Test
  public void makePartitionName_emptySpec() {

    String partitionName = makePartitionName(ImmutableList.of(), ImmutableList.of());

    assertTrue(partitionName.isEmpty());
  }

  @Test
  public void makePartitionName_partitionWithMissedValue() {
    String partitionName =
        makePartitionName(ImmutableList.of("part1", "part2"), ImmutableList.of(""));

    assertTrue(partitionName.isEmpty());
  }

  @Test
  public void makePartitionName_onePartition() {
    String partitionName = makePartitionName(ImmutableList.of("part"), ImmutableList.of("1"));

    assertEquals("part=1", partitionName);
  }

  @Test
  public void makePartitionName_multiplePartitions() {
    String partitionName =
        makePartitionName(
            ImmutableList.of("part1", "part2", "part3"), ImmutableList.of("foo", "bar", "123"));

    assertEquals("part1=foo/part2=bar/part3=123", partitionName);
  }

  @Test
  public void makePartitionName_partitionWithEscape() {
    String partitionName =
        makePartitionName(
            ImmutableList.of("foo/bar", "\\baz"), ImmutableList.of("test^ #value", "\n\r"));

    assertEquals("foo%2Fbar=test%5E+%23value/%5Cbaz=%0A%0D", partitionName);
  }
}
