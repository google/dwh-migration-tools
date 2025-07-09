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
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.edwmigration.permissions.commands.expand.RuleSetCompiler;
import com.google.edwmigration.permissions.commands.expand.StreamProcessor;
import com.google.edwmigration.permissions.models.IamBinding;
import com.google.edwmigration.permissions.models.Principal;
import com.google.edwmigration.permissions.models.Rule;
import com.google.edwmigration.permissions.models.Table;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat.Policy;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat.Policy.PolicyItem;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat.Policy.PolicyItemAccess;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat.Policy.PolicyResource;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat.Service;
import com.google.edwmigration.permissions.utils.AbstractRuleSetMapper;
import com.google.edwmigration.permissions.utils.SimpleStreamProcessor;
import com.google.errorprone.annotations.ForOverride;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRangerPermissionMapper
    extends AbstractRuleSetMapper<AbstractRangerPermissionMapper.Context, IamBinding> {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractRangerHiveToIamBindingMapper.class);

  private static final String RANGER_SOURCE = "ranger";

  private static final RuleSetCompiler RULE_SET_COMPILER =
      new RuleSetCompiler(
          /* variables= */ ImmutableList.of(
              "table", "principal", "source_principal", "permission"));

  protected static final PolicyResource ANY_RESOURCE =
      PolicyResource.create(ImmutableList.of("*"), false, false);

  public enum RangerType {
    USER,
    GROUP,
    ROLE
  }

  @AutoValue
  public abstract static class Context {

    public static Context create(
        Table table,
        Principal principal,
        RangerPrincipal sourcePrincipal,
        Policy sourcePolicy,
        IamBinding iamBinding) {
      return new AutoValue_AbstractRangerPermissionMapper_Context(
          table, principal, sourcePrincipal, sourcePolicy, iamBinding);
    }

    public abstract Table table();

    public abstract Principal principal();

    public abstract RangerPrincipal sourcePrincipal();

    public abstract Policy sourcePolicy();

    public abstract IamBinding permission();

    ImmutableMap<String, Object> toMap() {
      return ImmutableMap.of(
          "table", table(),
          "principal", principal(),
          "source_principal", sourcePrincipal(),
          "source_permissions", ImmutableList.of(sourcePolicy()),
          "permission", permission());
    }
  }

  @AutoValue
  @JsonSerialize(as = RangerPrincipal.class)
  public abstract static class RangerPrincipal {

    public static RangerPrincipal create(RangerType type, String name) {
      return new AutoValue_AbstractRangerPermissionMapper_RangerPrincipal(type, name);
    }

    @JsonProperty
    public abstract RangerType type();

    @JsonProperty
    public abstract String name();
  }

  private static class PolicyState {

    private boolean validated;

    public PolicyState() {
      this.validated = false;
    }

    public boolean isValidated() {
      return validated;
    }

    public void setValidated(boolean validated) {
      this.validated = validated;
    }
  }

  public static final int RANGER_DEFAULT_PRIORITY = 0;

  private final String rangerPlugin;

  protected final StreamProcessor<Table> tableReader;

  protected final ImmutableMap<RangerPrincipal, Principal> principals;

  protected final ImmutableList<Service> services;

  protected final ImmutableList<Policy> policies;

  AbstractRangerPermissionMapper(
      String rangerPlugin,
      ImmutableList<Rule> rules,
      StreamProcessor<Table> tableReader,
      StreamProcessor<Principal> principalReader,
      StreamProcessor<Policy> rangerPolicyReader,
      StreamProcessor<Service> rangerServiceReader,
      String rangerServiceType) {
    super(IamBinding.class, "Ranger " + rangerPlugin, RULE_SET_COMPILER, rules);
    this.rangerPlugin = rangerPlugin;
    this.tableReader = tableReader;

    // Create a ranger principal to IAM principal map.
    principals =
        principalReader.process(
            stream ->
                stream
                    .flatMap(
                        principal ->
                            principal.sources().entries().stream()
                                .filter(entry -> entry.getKey().startsWith(RANGER_SOURCE + "/"))
                                .map(
                                    entry ->
                                        new SimpleEntry<>(
                                            RangerPrincipal.create(
                                                rangerSourceToType(entry.getKey()),
                                                entry.getValue()),
                                            principal)))
                    .collect(toImmutableMap(SimpleEntry::getKey, SimpleEntry::getValue)));

    services =
        rangerServiceReader.process(
            serviceStream ->
                serviceStream
                    .filter(service -> rangerServiceType.equals(service.type()))
                    .collect(toImmutableList()));

    ImmutableSet<String> serviceNames =
        services.stream().map(Service::name).collect(toImmutableSet());

    policies =
        rangerPolicyReader.process(
            policyStream ->
                policyStream
                    .filter(policy -> serviceNames.contains(policy.service()))
                    .collect(toImmutableList()));
  }

  @Override
  protected ImmutableMap<String, Object> toBaseObjectMap(Context context) {
    return ImmutableMap.of(
        "source_permissions",
        ImmutableMultimap.of(RANGER_SOURCE, context.sourcePolicy()),
        "source_principals",
        ImmutableMultimap.of(RANGER_SOURCE, context.sourcePrincipal()));
  }

  @Override
  protected ImmutableMap<String, Object> toDefaultObjectMap(Context context) {
    return ImmutableMap.<String, Object>builder()
        .putAll(
            ImmutableMap.of(
                "priority", RANGER_DEFAULT_PRIORITY,
                "principal", context.permission().principal(),
                "role", context.permission().role()))
        .putAll(mapContextToDefaultObjectMap(context))
        .build();
  }

  @ForOverride
  protected abstract ImmutableList<IamBinding.Builder> mapTableToIamBinding(
      IamBinding.Builder builder, Table table);

  protected final ImmutableMap<String, Object> mapContextToDefaultObjectMap(Context context) {
    return ImmutableMap.of(
        "resourceType", context.permission().resourceType(),
        "resourcePath", context.permission().resourcePath());
  }

  @Override
  protected ImmutableMap<String, Object> toContext(Context context) {
    return context.toMap();
  }

  @Override
  protected StreamProcessor<Context> newStream() {
    Map<Policy, PolicyState> policyStats = new HashMap<>();

    // Table's cardinality might be high, so we iterate it as a stream.
    Stream<Context> permissionContextes =
        tableReader.process(
            tableStream ->
                tableStream.flatMap(
                    table ->
                        // Cross product with matching policies.
                        policies.stream()
                            .filter(policy -> policyMatchesTable(policy, table))
                            .flatMap(
                                policy ->
                                    // Cross product with policy items.
                                    policy.policyItems().stream()
                                        .flatMap(
                                            policyItem ->
                                                // Cross product with policy principals.
                                                getRangerPrincipalsForPolicyItem(policyItem)
                                                    .flatMap(
                                                        rangerPrincipal ->
                                                            createPermissionContext(
                                                                policy,
                                                                policyStats.computeIfAbsent(
                                                                    policy,
                                                                    key -> new PolicyState()),
                                                                policyItem,
                                                                table,
                                                                principals,
                                                                rangerPrincipal)
                                                                .stream())))));
    return new SimpleStreamProcessor<>(permissionContextes);
  }

  protected <T> Set<T> intersection(Set<T> lhs, Set<T> rhs) {
    Set<T> result = new HashSet<>(lhs);
    result.retainAll(rhs);
    return result;
  }

  private List<Context> createPermissionContext(
      Policy policy,
      PolicyState policyState,
      PolicyItem policyItem,
      Table table,
      ImmutableMap<RangerPrincipal, Principal> principals,
      RangerPrincipal rangerPrincipal) {
    Principal principal = principals.get(rangerPrincipal);
    if (principal == null) {
      throw new IllegalArgumentException(
          String.format(
              "Ranger %s policy '%s' refers to an invalid Ranger principal '%s'",
              rangerPlugin, policy.name(), rangerPrincipal));
    }
    if (principal.action() == Action.SKIP) {
      LOG.warn(
          "Ranger {} policy '{}' ignored for skipped Ranger principal '{}'",
          rangerPlugin,
          policy.name(),
          rangerPrincipal);
      return ImmutableList.of();
    }
    Optional<String> role = getRoleForAccesses(policyItem.accesses());
    if (!role.isPresent()) {
      LOG.warn("Ranger {} policy '{}' ignored due to accesses", rangerPlugin, policy.name());
      return ImmutableList.of();
    }
    if (policyState.isValidated()) {
      validatePolicy(policy);
      policyState.setValidated(true);
    }
    ImmutableList<IamBinding.Builder> iamBindingBuilders =
        mapTableToIamBinding(
            IamBinding.builder()
                .principal(principal.type().jsonValue() + ":" + principal.emailAddress())
                .role(role.get())
                .priority(RANGER_DEFAULT_PRIORITY),
            table);
    return iamBindingBuilders.stream()
        .map(
            iamBindingBuilder ->
                Context.create(
                    table, principal, rangerPrincipal, policy, iamBindingBuilder.build()))
        .collect(toImmutableList());
  }

  private void validatePolicy(Policy policy) {
    // TODO(aleofreddi): improve rejection handling, we should save these somewhere.
    // Add more
    // checks for unsupported features, like time-based policies, exceptions, deny
    // list.
    if (!policy.allowExceptions().isEmpty()) {
      LOG.warn(
          "Allow exceptions are not supported for Ranger {} policy {}",
          rangerPlugin,
          policy.name());
    }
    if (!policy.denyPolicyItems().isEmpty()) {
      LOG.warn("Deny items are not supported for Ranger {} policy {}", rangerPlugin, policy.name());
    }
    if (!policy.denyExceptions().isEmpty()) {
      LOG.warn(
          "Deny exceptions are not supported for Ranger {} policy {}", rangerPlugin, policy.name());
    }
    if (isNullOrEmpty(policy.rowFilterPolicyItems())) {
      LOG.warn(
          "Row filter policies are not supported for Ranger {} policy {}",
          rangerPlugin,
          policy.name());
    }
    if (isNullOrEmpty(policy.dataMaskPolicyItems())) {
      LOG.warn(
          "Data mask policies are not supported for Ranger {} policy {}",
          rangerPlugin,
          policy.name());
    }
  }

  private Stream<RangerPrincipal> getRangerPrincipalsForPolicyItem(PolicyItem policyItem) {
    return Stream.of(
            streamOrEmpty(policyItem.users())
                .map(rangerUser -> RangerPrincipal.create(RangerType.USER, rangerUser)),
            streamOrEmpty(policyItem.groups())
                .map(rangerGroup -> RangerPrincipal.create(RangerType.GROUP, rangerGroup)),
            streamOrEmpty(policyItem.roles())
                .map(rangerRole -> RangerPrincipal.create(RangerType.ROLE, rangerRole)))
        .flatMap(stream -> stream);
  }

  @ForOverride
  protected abstract boolean policyMatchesTable(Policy policy, Table table);

  @ForOverride
  protected abstract Optional<String> getRoleForAccesses(List<PolicyItemAccess> accesses);

  private static RangerType rangerSourceToType(String rangerSource) {
    return RangerType.valueOf(rangerSource.split("/")[1].toUpperCase());
  }

  private <T> Stream<T> streamOrEmpty(Collection<T> collection) {
    if (collection == null) {
      return Stream.empty();
    }
    return collection.stream();
  }

  private static boolean isNullOrEmpty(Collection<?> collection) {
    return collection == null || collection.isEmpty();
  }
}
