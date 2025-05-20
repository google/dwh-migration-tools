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

import static com.google.common.collect.ImmutableMap.toImmutableMap;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.edwmigration.permissions.commands.expand.RuleSetCompiler;
import com.google.edwmigration.permissions.commands.expand.StreamProcessor;
import com.google.edwmigration.permissions.models.HdfsPermission;
import com.google.edwmigration.permissions.models.IamBinding;
import com.google.edwmigration.permissions.models.Principal;
import com.google.edwmigration.permissions.models.Rule;
import com.google.edwmigration.permissions.models.Table;
import com.google.edwmigration.permissions.utils.SimpleStreamProcessor;
import com.google.errorprone.annotations.ForOverride;
import java.net.URI;
import java.util.AbstractMap.SimpleEntry;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractHdfsToIamBindingMapper
    extends AbstractIamBindingMapper<AbstractHdfsToIamBindingMapper.Context> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractHdfsToIamBindingMapper.class);

  private static final String HDFS_SOURCE = "hdfs";

  private static final String HDFS_OTHER_NAME = "other";

  private static final RuleSetCompiler RULE_SET_COMPILER =
      new RuleSetCompiler(
          /* variables= */ ImmutableList.of(
              "table", "principal", "source_principal", "permission"));

  private static final int HDFS_DEFAULT_PRIORITY = 10;

  public enum HdfsPrincipalType {
    USER,
    GROUP,
    OTHER,
  }

  @AutoValue
  public abstract static class Context {

    public static Context create(
        Table table,
        Principal principal,
        HdfsPrincipal sourcePrincipal,
        HdfsPermission sourcePermission,
        IamBinding iamBinding) {
      return new AutoValue_AbstractHdfsToIamBindingMapper_Context(
          table, principal, sourcePrincipal, sourcePermission, iamBinding);
    }

    @JsonProperty
    public abstract Table table();

    @JsonProperty
    public abstract Principal principal();

    public abstract HdfsPrincipal sourcePrincipal();

    public abstract HdfsPermission sourcePermission();

    public abstract IamBinding permission();

    // FIXME: can be removed by using object mapper.
    ImmutableMap<String, Object> toMap() {
      return ImmutableMap.of(
          "table", table(),
          "principal", principal(),
          "source_principal", sourcePrincipal(),
          "source_permissions", ImmutableList.of(sourcePermission()),
          "permission", permission());
    }
  }

  @AutoValue
  @JsonSerialize(as = HdfsPrincipal.class)
  public abstract static class HdfsPrincipal {

    public static HdfsPrincipal create(HdfsPrincipalType type, String name) {
      return new AutoValue_AbstractHdfsToIamBindingMapper_HdfsPrincipal(type, name);
    }

    @JsonProperty
    public abstract HdfsPrincipalType type();

    @JsonProperty
    @Nullable
    public abstract String name();
  }

  private final StreamProcessor<Table> tableReader;

  private final StreamProcessor<HdfsPermission> permissionReader;

  private final ImmutableMap<HdfsPrincipal, Principal> principals;

  private final String iamReadRole;

  private final String iamWriteRole;

  AbstractHdfsToIamBindingMapper(
      ImmutableList<Rule> rules,
      StreamProcessor<Table> tableReader,
      StreamProcessor<Principal> principalReader,
      StreamProcessor<HdfsPermission> permissionReader,
      String iamReadRole,
      String iamWriteRole) {
    super(IamBinding.class, "HDFS", RULE_SET_COMPILER, rules);
    this.iamReadRole = iamReadRole;
    this.iamWriteRole = iamWriteRole;
    this.tableReader = tableReader;
    this.permissionReader = permissionReader;

    // Create a HDFS principal to IAM principal map.
    principals =
        principalReader.process(
            stream ->
                stream
                    .flatMap(
                        principal ->
                            principal.sources().entries().stream()
                                .filter(entry -> entry.getKey().startsWith(HDFS_SOURCE + "/"))
                                .map(
                                    entry ->
                                        new SimpleEntry<>(
                                            HdfsPrincipal.create(
                                                hdfsSourceToType(entry.getKey()), entry.getValue()),
                                            principal)))
                    .collect(toImmutableMap(SimpleEntry::getKey, SimpleEntry::getValue)));
  }

  @Override
  protected ImmutableMap<String, Object> toBaseObjectMap(Context context) {
    return ImmutableMap.of(
        "source_permissions",
        ImmutableMultimap.of(HDFS_SOURCE, context.sourcePermission()),
        "source_principals",
        ImmutableMultimap.of(HDFS_SOURCE, context.sourcePrincipal()));
  }

  @Override
  protected ImmutableMap<String, Object> toDefaultObjectMap(Context context) {
    return ImmutableMap.<String, Object>builder()
        .putAll(
            ImmutableMap.of(
                "priority", 0,
                "principal", context.permission().principal(),
                "role", context.permission().role()))
        .putAll(mapTableToDefaultObjectMap(context.table()))
        .build();
  }

  @ForOverride
  protected abstract IamBinding.Builder mapTableToIamBinding(
      IamBinding.Builder builder, Table table);

  @ForOverride
  protected abstract ImmutableMap<String, Object> mapTableToDefaultObjectMap(Table table);

  @Override
  protected ImmutableMap<String, Object> toContext(Context context) {
    return context.toMap();
  }

  @Override
  protected StreamProcessor<Context> newStream() {
    Stream<Context> permissionContextes =
        permissionReader.process(
            permissionStream ->
                tableReader.process(
                    tableStream -> {
                      // TODO(aleofreddi) Here we order the streams in memory and implement a
                      // basic form of merge join. If this proves to be too memory-intensive,
                      // we might want to perform an external sort on permissions and tables.
                      Iterator<HdfsPermission> permissionIterator =
                          permissionStream
                              .sorted(Comparator.comparing(HdfsPermission::path))
                              .iterator();
                      Iterator<Table> tableIterator =
                          tableStream.sorted(Comparator.comparing(Table::hdfsPath)).iterator();
                      return MatchingIterator.mergeJoinStream(
                              tableIterator, permissionIterator, this::compareTablePermissionPath)
                          .flatMap(
                              tablePermissionEntry ->
                                  // Cross product with permission principals.
                                  getHdfsPrincipalsForPolicyItem(tablePermissionEntry.getValue())
                                      .map(
                                          hdfsPrincipal ->
                                              createPermissionContext(
                                                  tablePermissionEntry.getValue(),
                                                  tablePermissionEntry.getKey(),
                                                  principals,
                                                  hdfsPrincipal)))
                          .filter(Optional::isPresent)
                          .map(Optional::get);
                    }));
    return new SimpleStreamProcessor<>(permissionContextes);
  }

  private Optional<Context> createPermissionContext(
      HdfsPermission permission,
      Table table,
      ImmutableMap<HdfsPrincipal, Principal> principals,
      HdfsPrincipal hdfsPrincipal) {
    Principal principal = principals.get(hdfsPrincipal);
    if (principal == null) {
      throw new IllegalArgumentException(
          String.format(
              "HDFS permission on %s refers to an invalid HDFS principal '%s'",
              permission.path(), hdfsPrincipal));
    }
    if (principal.action() == Action.SKIP) {
      LOG.warn(
          "HDFS permission on {} ignored due to skipped principal '{}'",
          permission.path(),
          hdfsPrincipal);
      return Optional.empty();
    }
    Optional<String> role = getRoleForPermission(permission.permission(), hdfsPrincipal.type());
    if (!role.isPresent()) {
      LOG.warn("HDFS permission on {} ignored due to accesses", permission.path());
      return Optional.empty();
    }
    IamBinding.Builder iamBindingBuilder =
        mapTableToIamBinding(
            IamBinding.builder()
                .principal(principal.type().jsonValue() + ":" + principal.emailAddress())
                .role(role.get())
                .priority(HDFS_DEFAULT_PRIORITY),
            table);
    return Optional.of(
        Context.create(table, principal, hdfsPrincipal, permission, iamBindingBuilder.build()));
  }

  private Stream<HdfsPrincipal> getHdfsPrincipalsForPolicyItem(HdfsPermission permission) {
    return Stream.of(
        HdfsPrincipal.create(HdfsPrincipalType.USER, permission.owner()),
        HdfsPrincipal.create(HdfsPrincipalType.GROUP, permission.group()),
        HdfsPrincipal.create(HdfsPrincipalType.OTHER, HDFS_OTHER_NAME));
  }

  private Optional<String> getRoleForPermission(String permissions, HdfsPrincipalType type) {
    switch (type) {
      case USER:
        return getRoleForPermission(permissions.substring(0, 3));
      case GROUP:
        return getRoleForPermission(permissions.substring(3, 6));
      case OTHER:
        return getRoleForPermission(permissions.substring(6, 9));
    }
    throw new IllegalStateException("Unsupported HDFS principal type " + type);
  }

  private Optional<String> getRoleForPermission(String permissions) {
    if (permissions.charAt(1) == 'w') {
      return Optional.of(iamWriteRole);
    }
    if (permissions.charAt(0) == 'r') {
      return Optional.of(iamReadRole);
    }
    return Optional.empty();
  }

  private int compareTablePermissionPath(Table table, HdfsPermission permission) {
    String tablePath = URI.create(table.hdfsPath()).getPath();
    return tablePath.compareTo(permission.path());
  }

  private static HdfsPrincipalType hdfsSourceToType(String rangerSource) {
    return HdfsPrincipalType.valueOf(rangerSource.split("/")[1].toUpperCase());
  }
}
