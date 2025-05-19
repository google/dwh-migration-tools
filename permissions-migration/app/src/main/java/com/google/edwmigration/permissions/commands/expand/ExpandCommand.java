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

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.permissions.files.FileProcessor;
import com.google.edwmigration.permissions.models.Principal;
import com.google.edwmigration.permissions.models.PrincipalRuleset;
import com.google.edwmigration.permissions.models.PrincipalRuleset.HdfsPrincipalRuleset;
import com.google.edwmigration.permissions.models.PrincipalRuleset.RangerPrincipalRuleset;
import com.google.edwmigration.permissions.models.Principals;
import com.google.edwmigration.permissions.utils.RuleSetMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExpandCommand {

  private static final Logger LOG = LoggerFactory.getLogger(ExpandCommand.class);

  public void run(String[] args) throws IOException {
    ExpandOptions options = new ExpandOptions(args);
    if (options.handleHelp()) {
      return;
    }

    PrincipalRuleset config = parse(options);
    String dumperHdfs = options.getDumperHdfs();
    String dumperRanger = options.getDumperRanger();

    List<RuleSetMapper<Principal>> mappers = new ArrayList<>();

    // Handle HDFS principals.
    if (config.hdfsPrincipalRuleset() != null) {
      HdfsPrincipalRuleset hdfsRuleset = config.hdfsPrincipalRuleset();
      mappers.addAll(
          Stream.of(
                  Optional.ofNullable(hdfsRuleset.userRules())
                      .map(
                          rules ->
                              new HdfsUserPrincipalMapper(rules, new HdfsUserReader(dumperHdfs))),
                  Optional.ofNullable(hdfsRuleset.groupRules())
                      .map(
                          rules ->
                              new HdfsGroupPrincipalMapper(rules, new HdfsGroupReader(dumperHdfs))),
                  Optional.ofNullable(hdfsRuleset.otherRules()).map(HdfsOtherPrincipalMapper::new))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .collect(toImmutableList()));
    }

    // Handle Ranger principals.
    if (config.rangerPrincipalRuleset() != null) {
      RangerPrincipalRuleset rangerRuleset = config.rangerPrincipalRuleset();
      mappers.addAll(
          Stream.of(
                  Optional.ofNullable(rangerRuleset.userRules())
                      .map(
                          rules ->
                              new RangerUserPrincipalsMapper(
                                  rules, new RangerUserReader(dumperRanger)) {}),
                  Optional.ofNullable(rangerRuleset.groupRules())
                      .map(
                          rules ->
                              new RangerGroupPrincipalsMapper(
                                  rules, new RangerGroupReader(dumperRanger)) {}),
                  Optional.ofNullable(rangerRuleset.roleRules())
                      .map(
                          rules ->
                              new RangerRolePrincipalsMapper(
                                  rules, new RangerRoleReader(dumperRanger)) {}))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .collect(toImmutableList()));
    }
    PrincipalMerge principalMerge = new PrincipalMerge(mappers);
    ImmutableList<Principal> principals = principalMerge.run();
    FileProcessor.applyConsumer(
        options.getOutputPrincipals(),
        path ->
            Files.write(
                path, Principals.YAML_MAPPER.writeValueAsBytes(Principals.create(principals))));
  }

  PrincipalRuleset parse(ExpandOptions opts) {
    return FileProcessor.apply(
        opts.getPrincipalRuleset(),
        path ->
            PrincipalRuleset.YAML_MAPPER.readValue(
                Files.readAllBytes(path), PrincipalRuleset.class));
  }
}
