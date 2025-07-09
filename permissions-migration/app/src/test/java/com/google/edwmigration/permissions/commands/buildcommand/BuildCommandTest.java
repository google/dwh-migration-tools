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
package com.google.edwmigration.permissions.commands.buildcommand;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.permissions.commands.buildcommand.BuildCommand.PermissionsRulesetParser;
import com.google.edwmigration.permissions.commands.buildcommand.BuildCommand.PrincipalReaderFactory;
import com.google.edwmigration.permissions.commands.buildcommand.BuildCommand.TableReaderFactory;
import com.google.edwmigration.permissions.files.FileProcessor;
import com.google.edwmigration.permissions.models.IamBinding;
import com.google.edwmigration.permissions.models.PermissionsRuleset;
import com.google.edwmigration.permissions.models.PermissionsRuleset.BqPermissionsRuleset;
import com.google.edwmigration.permissions.models.PermissionsRuleset.GcsPermissionsRuleset;
import com.google.edwmigration.permissions.models.Rule;
import com.google.edwmigration.permissions.utils.RuleSetMapper;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class BuildCommandTest {

  @ParameterizedTest
  @MethodSource("rulesetProvider")
  public void run_noHdfsPath_hdfsMappersNotCreated(
      ImmutableList<Rule> gcsHdfsMappingRules, ImmutableList<Rule> bqHdfsMappingRules)
      throws Exception {
    PermissionsRulesetParser permissionsRulesetParser = mock(PermissionsRulesetParser.class);
    when(permissionsRulesetParser.parse(any()))
        .thenReturn(
            PermissionsRuleset.create(
                GcsPermissionsRuleset.create(gcsHdfsMappingRules, null, null),
                BqPermissionsRuleset.create(bqHdfsMappingRules, null, null),
                ImmutableMap.of()));

    TableReaderFactory tableReaderFactory = mock(TableReaderFactory.class);
    TableReader tableReader = mock(TableReader.class);
    when(tableReader.process(any())).thenReturn(null);
    when(tableReaderFactory.getInstance(any())).thenReturn(tableReader);

    PrincipalReaderFactory principalReaderFactory = mock(PrincipalReaderFactory.class);
    PrincipalReader principalReader = mock(PrincipalReader.class);
    when(principalReader.process(any())).thenReturn(null);
    when(principalReaderFactory.getInstance(any())).thenReturn(principalReader);

    BuildCommand buildCommand =
        new BuildCommand(permissionsRulesetParser, tableReaderFactory, principalReaderFactory);

    String[] args = {
      "--" + BuildOptions.OPT_DUMPER_RANGER, "ranger_dump_dummy_path",
      "--" + BuildOptions.OPT_PRINCIPALS, "principals_dummy_path",
      "--" + BuildOptions.OPT_PERMISSIONS_RULESET, "ruleset_dummy_path",
      "--" + BuildOptions.OPT_TABLES, "tables_dummy_path"
    };

    PermissionMerge mockedPermissionMerge = mock(PermissionMerge.class);

    try (MockedStatic<PermissionMerge> permissionMergeMockHandle =
            Mockito.mockStatic(PermissionMerge.class);
        MockedStatic<FileProcessor> fileProcessorMockHandle =
            Mockito.mockStatic(FileProcessor.class)) {
      permissionMergeMockHandle
          .when(() -> PermissionMerge.newInstance(any(ImmutableList.class), anyMap()))
          .thenAnswer(
              invocation -> {
                ImmutableList<RuleSetMapper<IamBinding>> mappersList = invocation.getArgument(0);
                assertThat(mappersList.size(), is(0));
                return mockedPermissionMerge;
              });

      buildCommand.run(args);
    }
  }

  private static Stream<Arguments> rulesetProvider() {
    return Stream.of(
        Arguments.of(ImmutableList.of(Rule.builder().build()), null),
        Arguments.of(null, ImmutableList.of(Rule.builder().build())));
  }
}
