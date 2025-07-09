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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.edwmigration.permissions.ProcessingException;
import com.google.edwmigration.permissions.models.IamBinding;
import com.google.edwmigration.permissions.models.PermissionsRuleset.RoleMapping;
import com.google.edwmigration.permissions.utils.Mapper;
import com.google.edwmigration.permissions.utils.RuleSetMapper;
import com.google.edwmigration.permissions.utils.RuleSetMapper.Action;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Merges the results of one or more permission mappers. */
class PermissionMerge implements Mapper<IamBinding> {

  private static final Logger LOG = LoggerFactory.getLogger(PermissionMerge.class);

  private final List<RuleSetMapper<IamBinding>> mappers;

  private final ImmutableMap<String, RoleMapping> roleMappings;

  private final ImmutableMultimap<String, String> roleSuperset;

  private PermissionMerge(
      ImmutableList<RuleSetMapper<IamBinding>> mappers, Map<String, RoleMapping> roles) {
    roleMappings = Optional.ofNullable(roles).map(ImmutableMap::copyOf).orElse(ImmutableMap.of());
    // Builds a map of all the roles included by other (possibly multiple) roles.
    ArrayListMultimap<String, String> roleSuperset =
        ArrayListMultimap.create(
            roleMappings.entrySet().stream()
                .flatMap(
                    entry ->
                        entry.getValue().includes() == null
                            ? Stream.of()
                            : entry.getValue().includes().stream()
                                .map(include -> new SimpleEntry<>(include, entry.getKey())))
                .collect(
                    ImmutableListMultimap.toImmutableListMultimap(
                        SimpleEntry::getKey, SimpleEntry::getValue)));
    // Compute the transitive closure of the role-superset relationship.
    for (int size = 0; size != roleSuperset.size(); size = roleSuperset.size()) {
      ImmutableSet.copyOf(roleSuperset.entries()).stream()
          .flatMap(
              entry ->
                  roleSuperset.get(entry.getValue()).stream()
                      .map(superset -> new SimpleEntry<>(entry.getKey(), superset)))
          .forEach(entry -> roleSuperset.put(entry.getKey(), entry.getValue()));
    }
    this.roleSuperset = ImmutableMultimap.copyOf(roleSuperset);
    this.mappers = mappers;
    checkArgument(!mappers.isEmpty(), "At least one permission mapper should be defined");
  }

  static PermissionMerge newInstance(
      ImmutableList<RuleSetMapper<IamBinding>> mappers, Map<String, RoleMapping> roles) {
    return new PermissionMerge(mappers, roles);
  }

  public ImmutableList<IamBinding> run() {
    return mappers.stream()
        .flatMap(expander -> runSingleMapper(expander).stream())
        .filter(result -> result.action() == Action.MAP)
        .map(RuleSetMapper.Result::value)
        .collect(groupingBy(this::getTarget, groupingBy(IamBinding::priority)))
        .entrySet()
        .stream()
        .flatMap(this::mergePermissions)
        .collect(toImmutableList());
  }

  private String getTarget(IamBinding iamBinding) {
    return iamBinding.resourceType().name() + ":" + iamBinding.resourcePath();
  }

  private Stream<IamBinding> mergePermissions(
      Entry<String, Map<Integer, List<IamBinding>>> pathPermissions) {
    // Group by priority, identify the set of permissions matching the highest priority.
    Map<Integer, List<IamBinding>> permissionsByPriority = pathPermissions.getValue();
    if (permissionsByPriority.isEmpty()) {
      return Stream.of();
    }
    int highestPriority = permissionsByPriority.keySet().stream().max(Integer::compareTo).get();
    ImmutableMultimap<String, IamBinding> pathPrincipalPermissions =
        permissionsByPriority.get(highestPriority).stream()
            .collect(
                ImmutableListMultimap.toImmutableListMultimap(IamBinding::principal, identity()));
    return pathPrincipalPermissions.asMap().entrySet().stream()
        .flatMap(this::mergePathPrincipalPermissions);
  }

  private Stream<IamBinding> mergePathPrincipalPermissions(
      Entry<String, Collection<IamBinding>> pathPermissions) {
    // Maps permissions by role.
    ImmutableMap<String, IamBinding> roles =
        pathPermissions.getValue().stream()
            .collect(
                toImmutableMap(
                    IamBinding::role,
                    identity(),
                    // On conflict, pick the permission with the highest priority.
                    (permission1, permission2) ->
                        permission1.priority() > permission2.priority()
                            ? permission1
                            : permission2));
    return roles.values().stream()
        // If the given role is included in another one, skip it.
        .filter(
            permission ->
                !roleSuperset.containsKey(permission.role())
                    || roleSuperset.get(permission.role()).stream().noneMatch(roles::containsKey))
        // Rename/expand role if `renameTo` is present.
        .flatMap(
            permission ->
                Optional.ofNullable(
                        roleMappings
                            .getOrDefault(permission.role(), RoleMapping.create(null, null))
                            .renameTo())
                    .orElse(ImmutableList.of(permission.role())).stream()
                    .map(
                        renamedRole ->
                            IamBinding.builder()
                                .resourceType(permission.resourceType())
                                .resourcePath(permission.resourcePath())
                                .principal(permission.principal())
                                .role(renamedRole)
                                .priority(permission.priority())
                                .sourcePermissions(permission.sourcePermissions())
                                .sourcePrincipals(permission.sourcePrincipals())
                                .build()));
  }

  private ImmutableList<RuleSetMapper.Result<IamBinding>> runSingleMapper(
      RuleSetMapper<IamBinding> mapper) {
    try {
      return mapper.run();
    } catch (ProcessingException e) {
      LOG.error("Exception when running mapper {}: ", mapper.getClass(), e);
      return ImmutableList.of();
    }
  }
}
