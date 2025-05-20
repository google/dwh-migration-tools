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

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.permissions.files.FileProcessor;
import com.google.edwmigration.permissions.models.IamBinding;
import com.google.edwmigration.permissions.models.Permissions;
import com.google.edwmigration.permissions.models.PermissionsRuleset;
import com.google.edwmigration.permissions.models.PermissionsRuleset.BqPermissionsRuleset;
import com.google.edwmigration.permissions.models.PermissionsRuleset.GcsPermissionsRuleset;
import com.google.edwmigration.permissions.utils.RuleSetMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BuildCommand {

  private static final Logger LOG = LoggerFactory.getLogger(BuildCommand.class);

  PermissionsRulesetParser permissionsRulesetParser;

  TableReaderFactory tableReaderFactory;

  PrincipalReaderFactory principalReaderFactory;

  public BuildCommand() {
    this(
        new PermissionsRulesetParserImpl(),
        new TableReaderFactoryImpl(),
        new PrincipalReaderFactoryImpl());
  }

  BuildCommand(
      PermissionsRulesetParser permissionsRulesetParser,
      TableReaderFactory tableReaderFactory,
      PrincipalReaderFactory principalReaderFactory) {
    this.permissionsRulesetParser = permissionsRulesetParser;
    this.tableReaderFactory = tableReaderFactory;
    this.principalReaderFactory = principalReaderFactory;
  }

  public void run(String[] args) throws IOException {
    BuildOptions options = new BuildOptions(args);
    if (options.handleHelp()) {
      return;
    }

    PermissionsRuleset config = permissionsRulesetParser.parse(options);

    List<RuleSetMapper<IamBinding>> mappers = new ArrayList<>();

    // Handle GCS permissions.
    if (config.gcsPermissionsRuleset() != null) {
      GcsPermissionsRuleset gcsRuleset = config.gcsPermissionsRuleset();

      mappers.addAll(
          Stream.of(
                  Optional.ofNullable(gcsRuleset.rangerHiveMappingRules())
                      .map(
                          rules ->
                              new RangerHiveToGcsIamBindingMapper(
                                  rules,
                                  tableReaderFactory.getInstance(options),
                                  principalReaderFactory.getInstance(options),
                                  new RangerPolicyReader(options.getDumperRanger()),
                                  new RangerServiceReader(options.getDumperRanger()))),
                  Optional.ofNullable(gcsRuleset.rangerHiveHdfsMappingRules())
                      .map(
                          rules ->
                              new RangerHdfsToGcsIamBindingMapper(
                                  rules,
                                  tableReaderFactory.getInstance(options),
                                  principalReaderFactory.getInstance(options),
                                  new RangerPolicyReader(options.getDumperRanger()),
                                  new RangerServiceReader(options.getDumperRanger()))),
                  Optional.ofNullable(gcsRuleset.hdfsMappingRules())
                      .map(
                          rules -> {
                            if (options.getDumperHdfs() == null) {
                              LOG.warn(
                                  "HDFS rules present in the ruleset, but no HDFS dump file provided. Rules will be ignored");
                              return null;
                            }
                            return new HdfsToGcsIamBindingMapper(
                                rules,
                                tableReaderFactory.getInstance(options),
                                principalReaderFactory.getInstance(options),
                                new HdfsPermissionReader(options.getDumperHdfs()));
                          }))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .collect(toImmutableList()));
    }

    // Handle BQ permissions.
    if (config.bqPermissionsRuleset() != null) {
      BqPermissionsRuleset bqRuleset = config.bqPermissionsRuleset();
      mappers.addAll(
          Stream.of(
                  Optional.ofNullable(bqRuleset.rangerHiveMappingRules())
                      .map(
                          rules ->
                              new RangerHiveToBqTableIamBindingMapper(
                                  rules,
                                  tableReaderFactory.getInstance(options),
                                  principalReaderFactory.getInstance(options),
                                  new RangerPolicyReader(options.getDumperRanger()),
                                  new RangerServiceReader(options.getDumperRanger()))),
                  Optional.ofNullable(bqRuleset.rangerHdfsMappingRules())
                      .map(
                          rules ->
                              new RangerHdfsToBqTableIamBindingMapper(
                                  rules,
                                  tableReaderFactory.getInstance(options),
                                  principalReaderFactory.getInstance(options),
                                  new RangerPolicyReader(options.getDumperRanger()),
                                  new RangerServiceReader(options.getDumperRanger()))),
                  Optional.ofNullable(bqRuleset.hdfsMappingRules())
                      .map(
                          rules -> {
                            if (options.getDumperHdfs() == null) {
                              LOG.warn(
                                  "HDFS rules present in the ruleset, but no HDFS dump file provided. Rules will be ignored");
                              return null;
                            }
                            return new HdfsToBqTableIamBindingMapper(
                                rules,
                                tableReaderFactory.getInstance(options),
                                principalReaderFactory.getInstance(options),
                                new HdfsPermissionReader(options.getDumperHdfs()));
                          }))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .collect(toImmutableList()));
    }

    PermissionMerge merge =
        PermissionMerge.newInstance(ImmutableList.copyOf(mappers), config.roleMappings());
    ImmutableList<IamBinding> iamBindings = merge.run();
    FileProcessor.applyConsumer(
        options.getOutputPermissions(),
        path ->
            Files.write(
                path, Permissions.YAML_MAPPER.writeValueAsBytes(Permissions.create(iamBindings))));
  }

  private static final class PermissionsRulesetParserImpl implements PermissionsRulesetParser {

    public PermissionsRuleset parse(BuildOptions opts) {
      return FileProcessor.apply(
          opts.getPermissionsRuleset(),
          path ->
              PermissionsRuleset.YAML_MAPPER.readValue(
                  Files.readAllBytes(path), PermissionsRuleset.class));
    }
  }

  interface PermissionsRulesetParser {

    PermissionsRuleset parse(BuildOptions opts);
  }

  interface TableReaderFactory {

    TableReader getInstance(final BuildOptions options);
  }

  private static final class TableReaderFactoryImpl implements TableReaderFactory {

    // TODO cache this? b/387223930
    @Override
    public TableReader getInstance(final BuildOptions options) {
      return new TableReader(
          options.getTables(), options.getNumThreads(), options.getTimeoutSeconds());
    }
  }

  interface PrincipalReaderFactory {

    PrincipalReader getInstance(final BuildOptions options);
  }

  private static final class PrincipalReaderFactoryImpl implements PrincipalReaderFactory {

    // TODO cache this? b/387223930
    @Override
    public PrincipalReader getInstance(final BuildOptions options) {
      return new PrincipalReader(options.getPrincipals());
    }
  }
}
