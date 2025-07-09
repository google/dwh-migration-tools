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
package com.google.edwmigration.permissions.commands.expand;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.permissions.ProcessingException;
import com.google.edwmigration.permissions.models.Principal;
import com.google.edwmigration.permissions.utils.Mapper;
import com.google.edwmigration.permissions.utils.RuleSetMapper;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Merges the results of one or more mappers. */
public class PrincipalMerge implements Mapper<Principal> {

  private static final Logger LOG = LoggerFactory.getLogger(PrincipalMerge.class);

  private final List<RuleSetMapper<Principal>> mappers;

  public PrincipalMerge(List<RuleSetMapper<Principal>> mappers) {
    this.mappers = ImmutableList.copyOf(mappers);
    checkArgument(!mappers.isEmpty(), "At least one IAM principal source should be defined");
  }

  public ImmutableList<Principal> run() {
    return mappers.stream()
        .flatMap(mapper -> runSingleMapper(mapper).stream())
        .map(RuleSetMapper.Result::value)
        .distinct()
        .collect(toImmutableList());
  }

  private ImmutableList<RuleSetMapper.Result<Principal>> runSingleMapper(
      RuleSetMapper<Principal> mapper) {
    try {
      return mapper.run();
    } catch (ProcessingException e) {
      LOG.error("Exception when running mapper {}: ", mapper.getClass(), e);
      return ImmutableList.of();
    }
  }
}
