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
package com.google.edwmigration.permissions.models.ranger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonFormat.Shape;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public interface RangerDumpFormat {

  ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .registerModule(new GuavaModule())
          .enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
          .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
          .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
          .disable(SerializationFeature.INDENT_OUTPUT)
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  interface ServicesFormat {

    String ZIP_ENTRY_NAME = "services.jsonl";
  }

  interface PoliciesFormat {

    String ZIP_ENTRY_NAME = "policies.jsonl";
  }

  interface UsersFormat {

    String ZIP_ENTRY_NAME = "users.jsonl";
  }

  interface GroupsFormat {

    String ZIP_ENTRY_NAME = "groups.jsonl";
  }

  interface RolesFormat {

    String ZIP_ENTRY_NAME = "roles.jsonl";
  }

  /**
   * A Ranger service. Based on server's model defined at
   * https://raw.githubusercontent.com/apache/ranger/master/agents-common/src/main/java/org/apache/ranger/plugin/model/RangerService.java.
   */
  @AutoValue
  @JsonSerialize(as = Service.class)
  abstract class Service {

    @JsonCreator
    public static Service create(
        @JsonProperty("id") Long id,
        @JsonProperty("guid") String guid,
        @JsonProperty("isEnabled") Boolean isEnabled,
        @JsonProperty("createdBy") String createdBy,
        @JsonProperty("updatedBy") String updatedBy,
        @JsonProperty("createTime") Instant createTime,
        @JsonProperty("updateTime") Instant updateTime,
        @JsonProperty("version") Long version,
        @JsonProperty("type") String type,
        @JsonProperty("name") String name,
        @JsonProperty("displayName") String displayName,
        @JsonProperty("description") String description,
        @JsonProperty("tagService") String tagService,
        @JsonProperty("configs") ImmutableMap<String, String> configs,
        @JsonProperty("policyVersion") Long policyVersion,
        @JsonProperty("policyUpdateTime") Instant policyUpdateTime,
        @JsonProperty("tagVersion") Long tagVersion,
        @JsonProperty("tagUpdateTime") Instant tagUpdateTime) {
      return builder()
          .id(id)
          .guid(guid)
          .isEnabled(isEnabled)
          .createdBy(createdBy)
          .updatedBy(updatedBy)
          .createTime(createTime)
          .updateTime(updateTime)
          .version(version)
          .type(type)
          .name(name)
          .displayName(displayName)
          .description(description)
          .tagService(tagService)
          .configs(configs)
          .policyVersion(policyVersion)
          .policyUpdateTime(policyUpdateTime)
          .tagVersion(tagVersion)
          .tagUpdateTime(tagUpdateTime)
          .build();
    }

    @JsonProperty
    public abstract long id();

    @JsonProperty
    @Nullable
    public abstract String guid();

    @JsonProperty
    @Nullable
    public abstract Boolean isEnabled();

    @JsonProperty
    @Nullable
    public abstract String createdBy();

    @JsonProperty
    @Nullable
    public abstract String updatedBy();

    @JsonProperty
    @JsonFormat(shape = Shape.STRING)
    @Nullable
    public abstract Instant createTime();

    @JsonProperty
    @JsonFormat(shape = Shape.STRING)
    @Nullable
    public abstract Instant updateTime();

    @JsonProperty
    @Nullable
    public abstract Long version();

    @JsonProperty
    @Nullable
    public abstract String type();

    @JsonProperty
    public abstract String name();

    @JsonProperty
    @Nullable
    public abstract String displayName();

    @JsonProperty
    @Nullable
    public abstract String description();

    @JsonProperty
    @Nullable
    public abstract String tagService();

    @JsonProperty
    @Nullable
    public abstract ImmutableMap<String, String> configs();

    @JsonProperty
    @Nullable
    public abstract Long policyVersion();

    @JsonProperty
    @JsonFormat(shape = Shape.STRING)
    @Nullable
    public abstract Instant policyUpdateTime();

    @JsonProperty
    @Nullable
    public abstract Long tagVersion();

    @JsonProperty
    @JsonFormat(shape = Shape.STRING)
    @Nullable
    public abstract Instant tagUpdateTime();

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder id(Long value);

      public abstract Builder guid(String value);

      public abstract Builder isEnabled(Boolean value);

      public abstract Builder createdBy(String value);

      public abstract Builder updatedBy(String value);

      public abstract Builder createTime(Instant value);

      public abstract Builder updateTime(Instant value);

      public abstract Builder version(Long value);

      public abstract Builder type(String value);

      public abstract Builder name(String value);

      public abstract Builder displayName(String value);

      public abstract Builder description(String value);

      public abstract Builder tagService(String value);

      public abstract Builder configs(Map<String, String> value);

      public abstract Builder policyVersion(Long value);

      public abstract Builder policyUpdateTime(Instant value);

      public abstract Builder tagVersion(Long value);

      public abstract Builder tagUpdateTime(Instant value);

      public abstract Service build();
    }

    public static Builder builder() {
      return new AutoValue_RangerDumpFormat_Service.Builder();
    }
  }

  /**
   * A Ranger user. Based on server's model defined at
   * https://raw.githubusercontent.com/apache/ranger/master/security-admin/src/main/java/org/apache/ranger/view/VXUser.java.
   */
  @AutoValue
  @JsonSerialize(as = User.class)
  abstract class User {

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder id(long value);

      public abstract Builder createDate(Instant value);

      public abstract Builder updateDate(Instant value);

      public abstract Builder owner(String value);

      public abstract Builder updatedBy(String value);

      public abstract Builder name(String value);

      public abstract Builder firstName(String value);

      public abstract Builder lastName(String value);

      public abstract Builder emailAddress(String value);

      public abstract Builder credStoreId(String value);

      public abstract Builder description(String value);

      public abstract Builder groupIdList(List<Long> value);

      public abstract Builder groupNameList(List<String> value);

      public abstract Builder status(Integer value);

      public abstract Builder isVisible(Integer value);

      public abstract Builder userSource(Integer value);

      public abstract Builder userRoleList(List<String> value);

      public abstract Builder otherAttributes(String value);

      public abstract Builder syncSource(String value);

      public abstract User build();
    }

    public static Builder builder() {
      return new AutoValue_RangerDumpFormat_User.Builder();
    }

    @JsonCreator
    public static User create(
        @JsonProperty("id") long id,
        @JsonProperty("createDate") Instant createDate,
        @JsonProperty("updateDate") Instant updateDate,
        @JsonProperty("owner") String owner,
        @JsonProperty("updatedBy") String updatedBy,
        @JsonProperty("name") String name,
        @JsonProperty("firstName") String firstName,
        @JsonProperty("lastName") String lastName,
        @JsonProperty("emailAddress") String emailAddress,
        @JsonProperty("credStoreId") String credStoreId,
        @JsonProperty("description") String description,
        @JsonProperty("groupIdList") ImmutableList<Long> groupIdList,
        @JsonProperty("groupNameList") ImmutableList<String> groupNameList,
        @JsonProperty("status") Integer status,
        @JsonProperty("isVisible") Integer isVisible,
        @JsonProperty("userSource") Integer userSource,
        @JsonProperty("userRoleList") ImmutableList<String> userRoleList,
        @JsonProperty("otherAttributes") String otherAttributes,
        @JsonProperty("syncSource") String syncSource) {
      return builder()
          .id(id)
          .createDate(createDate)
          .updateDate(updateDate)
          .owner(owner)
          .updatedBy(updatedBy)
          .name(name)
          .firstName(firstName)
          .lastName(lastName)
          .emailAddress(emailAddress)
          .credStoreId(credStoreId)
          .description(description)
          .groupIdList(groupIdList)
          .groupNameList(groupNameList)
          .status(status)
          .isVisible(isVisible)
          .userSource(userSource)
          .userRoleList(userRoleList)
          .otherAttributes(otherAttributes)
          .syncSource(syncSource)
          .build();
    }

    @JsonProperty
    public abstract long id();

    @JsonProperty
    @JsonFormat(shape = Shape.STRING)
    @Nullable
    public abstract Instant createDate();

    @JsonProperty
    @JsonFormat(shape = Shape.STRING)
    @Nullable
    public abstract Instant updateDate();

    @JsonProperty
    @Nullable
    public abstract String owner();

    @JsonProperty
    @Nullable
    public abstract String updatedBy();

    @JsonProperty
    public abstract String name();

    @JsonProperty
    @Nullable
    public abstract String firstName();

    @JsonProperty
    @Nullable
    public abstract String lastName();

    @JsonProperty
    @Nullable
    public abstract String emailAddress();

    @JsonProperty
    @Nullable
    public abstract String credStoreId();

    @JsonProperty
    @Nullable
    public abstract String description();

    @JsonProperty
    public abstract ImmutableList<Long> groupIdList();

    @JsonProperty
    public abstract ImmutableList<String> groupNameList();

    @JsonProperty
    @Nullable
    public abstract Integer status();

    @JsonProperty
    @Nullable
    public abstract Integer isVisible();

    @JsonProperty
    @Nullable
    public abstract Integer userSource();

    @JsonProperty
    public abstract ImmutableList<String> userRoleList();

    @JsonProperty
    @Nullable
    public abstract String otherAttributes();

    @JsonProperty
    @Nullable
    public abstract String syncSource();
  }

  /**
   * A Ranger role. Based on server's model defined at
   * https://raw.githubusercontent.com/apache/ranger/master/agents-common/src/main/java/org/apache/ranger/plugin/model/RangerRole.java
   */
  @AutoValue
  @JsonSerialize(as = Role.class)
  abstract class Role {

    @AutoValue
    @JsonSerialize(as = RoleMember.class)
    public abstract static class RoleMember {

      @AutoValue.Builder
      public abstract static class Builder {

        public abstract Builder name(String value);

        public abstract Builder isAdmin(boolean value);

        public abstract RoleMember build();
      }

      public static Builder builder() {
        return new AutoValue_RangerDumpFormat_Role_RoleMember.Builder();
      }

      @JsonCreator
      public static RoleMember create(
          @JsonProperty("name") String name, @JsonProperty("isAdmin") boolean isAdmin) {
        return builder().name(name).isAdmin(isAdmin).build();
      }

      @JsonProperty
      public abstract String name();

      @JsonProperty
      public abstract boolean isAdmin();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder id(Long value);

      public abstract Builder guid(String value);

      public abstract Builder isEnabled(Boolean value);

      public abstract Builder createdBy(String value);

      public abstract Builder updatedBy(String value);

      public abstract Builder createTime(Instant value);

      public abstract Builder updateTime(Instant value);

      public abstract Builder version(Long value);

      public abstract Builder name(String value);

      public abstract Builder description(String value);

      public abstract Builder options(Map<String, Object> value);

      public abstract Builder users(List<RoleMember> value);

      public abstract Builder groups(List<RoleMember> value);

      public abstract Builder roles(List<RoleMember> value);

      public abstract Builder createdByUser(String value);

      public abstract Role build();
    }

    public static Builder builder() {
      return new AutoValue_RangerDumpFormat_Role.Builder();
    }

    @JsonCreator
    public static Role create(
        @JsonProperty("id") Long id,
        @JsonProperty("guid") String guid,
        @JsonProperty("isEnabled") Boolean isEnabled,
        @JsonProperty("createdBy") String createdBy,
        @JsonProperty("updatedBy") String updatedBy,
        @JsonProperty("createTime") Instant createTime,
        @JsonProperty("updateTime") Instant updateTime,
        @JsonProperty("version") Long version,
        @JsonProperty("name") String name,
        @JsonProperty("description") String description,
        @JsonProperty("options") ImmutableMap<String, Object> options,
        @JsonProperty("users") ImmutableList<RoleMember> users,
        @JsonProperty("groups") ImmutableList<RoleMember> groups,
        @JsonProperty("roles") ImmutableList<RoleMember> roles,
        @JsonProperty("createdByUser") String createdByUser) {
      return builder()
          .id(id)
          .guid(guid)
          .isEnabled(isEnabled)
          .createdBy(createdBy)
          .updatedBy(updatedBy)
          .createTime(createTime)
          .updateTime(updateTime)
          .version(version)
          .name(name)
          .description(description)
          .options(options)
          .users(users)
          .groups(groups)
          .roles(roles)
          .createdByUser(createdByUser)
          .build();
    }

    @JsonProperty
    public abstract long id();

    @JsonProperty
    @Nullable
    public abstract String guid();

    @JsonProperty
    @Nullable
    public abstract Boolean isEnabled();

    @JsonProperty
    @Nullable
    public abstract String createdBy();

    @JsonProperty
    @Nullable
    public abstract String updatedBy();

    @JsonProperty
    @JsonFormat(shape = Shape.STRING)
    @Nullable
    public abstract Instant createTime();

    @JsonProperty
    @JsonFormat(shape = Shape.STRING)
    @Nullable
    public abstract Instant updateTime();

    @JsonProperty
    @Nullable
    public abstract Long version();

    @JsonProperty
    public abstract String name();

    @JsonProperty
    @Nullable
    public abstract String description();

    @JsonProperty
    @Nullable
    public abstract ImmutableMap<String, Object> options();

    @JsonProperty
    @Nullable
    public abstract ImmutableList<RoleMember> users();

    @JsonProperty
    @Nullable
    public abstract ImmutableList<RoleMember> groups();

    @JsonProperty
    @Nullable
    public abstract ImmutableList<RoleMember> roles();

    @JsonProperty
    @Nullable
    public abstract String createdByUser();
  }

  /**
   * A Ranger group. Based on server's model defined at
   * https://raw.githubusercontent.com/apache/ranger/master/security-admin/src/main/java/org/apache/ranger/view/VXGroup.java.
   */
  @AutoValue
  @JsonSerialize(as = Group.class)
  abstract class Group {

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder id(long value);

      public abstract Builder createDate(Instant value);

      public abstract Builder updateDate(Instant value);

      public abstract Builder owner(String value);

      public abstract Builder updatedBy(String value);

      public abstract Builder name(String value);

      public abstract Builder description(String value);

      public abstract Builder groupType(Integer value);

      public abstract Builder groupSource(Integer value);

      public abstract Builder credStoreId(String value);

      public abstract Builder isVisible(Integer value);

      public abstract Builder otherAttributes(String value);

      public abstract Builder syncSource(String value);

      public abstract Group build();
    }

    public static Builder builder() {
      return new AutoValue_RangerDumpFormat_Group.Builder();
    }

    @JsonCreator
    public static Group create(
        @JsonProperty("id") long id,
        @JsonProperty("createDate") Instant createDate,
        @JsonProperty("updateDate") Instant updateDate,
        @JsonProperty("owner") String owner,
        @JsonProperty("updatedBy") String updatedBy,
        @JsonProperty("name") String name,
        @JsonProperty("description") String description,
        @JsonProperty("groupType") Integer groupType,
        @JsonProperty("groupSource") Integer groupSource,
        @JsonProperty("credStoreId") String credStoreId,
        @JsonProperty("isVisible") Integer isVisible,
        @JsonProperty("otherAttributes") String otherAttributes,
        @JsonProperty("syncSource") String syncSource) {
      return builder()
          .id(id)
          .createDate(createDate)
          .updateDate(updateDate)
          .owner(owner)
          .updatedBy(updatedBy)
          .name(name)
          .description(description)
          .groupType(groupType)
          .groupSource(groupSource)
          .credStoreId(credStoreId)
          .isVisible(isVisible)
          .otherAttributes(otherAttributes)
          .syncSource(syncSource)
          .build();
    }

    @JsonProperty
    public abstract long id();

    @JsonProperty
    @JsonFormat(shape = Shape.STRING)
    @Nullable
    public abstract Instant createDate();

    @JsonProperty
    @JsonFormat(shape = Shape.STRING)
    @Nullable
    public abstract Instant updateDate();

    @JsonProperty
    @Nullable
    public abstract String owner();

    @JsonProperty
    @Nullable
    public abstract String updatedBy();

    @JsonProperty
    public abstract String name();

    @JsonProperty
    @Nullable
    public abstract String description();

    @JsonProperty
    @Nullable
    public abstract Integer groupType();

    @JsonProperty
    @Nullable
    public abstract Integer groupSource();

    @JsonProperty
    @Nullable
    public abstract String credStoreId();

    @JsonProperty
    @Nullable
    public abstract Integer isVisible();

    @JsonProperty
    @Nullable
    public abstract String otherAttributes();

    @JsonProperty
    @Nullable
    public abstract String syncSource();
  }

  /**
   * A Ranger policy. Based on server's model defined at
   * https://raw.githubusercontent.com/apache/ranger/master/agents-common/src/main/java/org/apache/ranger/plugin/model/RangerPolicy.java
   */
  @AutoValue
  @JsonSerialize(as = Policy.class)
  abstract class Policy {

    @AutoValue
    @JsonSerialize(as = PolicyResource.class)
    public abstract static class PolicyResource {

      @AutoValue.Builder
      public abstract static class Builder {

        public abstract Builder values(List<String> value);

        public abstract Builder isExcludes(Boolean value);

        public abstract Builder isRecursive(Boolean value);

        public abstract PolicyResource build();
      }

      public static Builder builder() {
        return new AutoValue_RangerDumpFormat_Policy_PolicyResource.Builder();
      }

      @JsonCreator
      public static PolicyResource create(
          @JsonProperty("values") ImmutableList<String> values,
          @JsonProperty("isExcludes") Boolean isExcludes,
          @JsonProperty("isRecursive") Boolean isRecursive) {
        return builder().values(values).isExcludes(isExcludes).isRecursive(isRecursive).build();
      }

      @JsonProperty
      public abstract ImmutableList<String> values();

      @JsonProperty
      @Nullable
      public abstract Boolean isExcludes();

      @JsonProperty
      @Nullable
      public abstract Boolean isRecursive();
    }

    @AutoValue
    @JsonSerialize(as = ItemCondition.class)
    public abstract static class ItemCondition {

      @AutoValue.Builder
      public abstract static class Builder {

        public abstract Builder type(String value);

        public abstract Builder values(List<String> value);

        public abstract ItemCondition build();
      }

      public static Builder builder() {
        return new AutoValue_RangerDumpFormat_Policy_ItemCondition.Builder();
      }

      @JsonCreator
      public static ItemCondition create(
          @JsonProperty("type") String type, @JsonProperty("values") ImmutableList<String> values) {
        return builder().type(type).values(values).build();
      }

      @JsonProperty
      public abstract String type();

      @JsonProperty
      public abstract ImmutableList<String> values();
    }

    @AutoValue
    @JsonSerialize(as = PolicyItemAccess.class)
    public abstract static class PolicyItemAccess {

      @AutoValue.Builder
      public abstract static class Builder {

        public abstract Builder type(String value);

        public abstract Builder isAllowed(Boolean value);

        public abstract PolicyItemAccess build();
      }

      public static Builder builder() {
        return new AutoValue_RangerDumpFormat_Policy_PolicyItemAccess.Builder();
      }

      @JsonCreator
      public static PolicyItemAccess create(
          @JsonProperty("type") String type, @JsonProperty("isAllowed") Boolean isAllowed) {
        return builder().type(type).isAllowed(isAllowed).build();
      }

      @JsonProperty("type")
      public abstract String type();

      @JsonProperty("isAllowed")
      @Nullable
      public abstract Boolean isAllowed();
    }

    @AutoValue
    @JsonSerialize(as = PolicyItem.class)
    public abstract static class PolicyItem {

      @AutoValue.Builder
      public abstract static class Builder {

        public abstract Builder accesses(List<PolicyItemAccess> value);

        public abstract Builder users(List<String> value);

        public abstract Builder groups(List<String> value);

        public abstract Builder roles(List<String> value);

        public abstract Builder conditions(List<ItemCondition> value);

        public abstract Builder delegateAdmin(Boolean value);

        public abstract PolicyItem build();
      }

      public static Builder builder() {
        return new AutoValue_RangerDumpFormat_Policy_PolicyItem.Builder();
      }

      @JsonCreator
      public static PolicyItem create(
          @JsonProperty("accesses") ImmutableList<PolicyItemAccess> accesses,
          @JsonProperty("users") ImmutableList<String> users,
          @JsonProperty("groups") ImmutableList<String> groups,
          @JsonProperty("roles") ImmutableList<String> roles,
          @JsonProperty("conditions") ImmutableList<ItemCondition> conditions,
          @JsonProperty("delegateAdmin") Boolean delegateAdmin) {
        return builder()
            .accesses(accesses)
            .users(users)
            .groups(groups)
            .roles(roles)
            .conditions(conditions)
            .build();
      }

      @JsonProperty
      public abstract ImmutableList<PolicyItemAccess> accesses();

      @JsonProperty
      @Nullable
      public abstract ImmutableList<String> users();

      @JsonProperty
      @Nullable
      public abstract ImmutableList<String> groups();

      @JsonProperty
      @Nullable
      public abstract ImmutableList<String> roles();

      @JsonProperty
      @Nullable
      public abstract ImmutableList<ItemCondition> conditions();

      @JsonProperty
      @Nullable
      public abstract Boolean delegateAdmin();
    }

    @AutoValue
    @JsonSerialize(as = DataMaskPolicyItem.class)
    public abstract static class DataMaskPolicyItem {

      @AutoValue
      @JsonSerialize(as = PolicyItemDataMaskInfo.class)
      public abstract static class PolicyItemDataMaskInfo {

        @AutoValue.Builder
        public abstract static class Builder {

          public abstract Builder dataMaskType(String value);

          public abstract Builder conditionExpr(String value);

          public abstract Builder valueExpr(String value);

          public abstract PolicyItemDataMaskInfo build();
        }

        public static Builder builder() {
          return new AutoValue_RangerDumpFormat_Policy_DataMaskPolicyItem_PolicyItemDataMaskInfo
              .Builder();
        }

        @JsonCreator
        public static PolicyItemDataMaskInfo create(
            @JsonProperty("dataMaskType") String dataMaskType,
            @JsonProperty("conditionExpr") String conditionExpr,
            @JsonProperty("valueExpr") String valueExpr) {
          return builder()
              .dataMaskType(dataMaskType)
              .conditionExpr(conditionExpr)
              .valueExpr(valueExpr)
              .build();
        }

        @JsonProperty
        public abstract String dataMaskType();

        @JsonProperty
        @Nullable
        public abstract String conditionExpr();

        @JsonProperty
        @Nullable
        public abstract String valueExpr();
      }

      @AutoValue.Builder
      public abstract static class Builder {

        public abstract Builder accesses(List<PolicyItemAccess> value);

        public abstract Builder users(List<String> value);

        public abstract Builder groups(List<String> value);

        public abstract Builder roles(List<String> value);

        public abstract Builder conditions(List<ItemCondition> value);

        public abstract Builder delegateAdmin(Boolean value);

        public abstract Builder dataMaskInfo(PolicyItemDataMaskInfo value);

        public abstract DataMaskPolicyItem build();
      }

      public static Builder builder() {
        return new AutoValue_RangerDumpFormat_Policy_DataMaskPolicyItem.Builder();
      }

      @JsonCreator
      public static DataMaskPolicyItem create(
          @JsonProperty("accesses") ImmutableList<PolicyItemAccess> accesses,
          @JsonProperty("users") ImmutableList<String> users,
          @JsonProperty("groups") ImmutableList<String> groups,
          @JsonProperty("roles") ImmutableList<String> roles,
          @JsonProperty("conditions") ImmutableList<ItemCondition> conditions,
          @JsonProperty("delegateAdmin") Boolean delegateAdmin,
          @JsonProperty("dataMaskInfo") PolicyItemDataMaskInfo policyItemDataMaskInfo) {
        return builder()
            .accesses(accesses)
            .users(users)
            .groups(groups)
            .roles(roles)
            .conditions(conditions)
            .delegateAdmin(delegateAdmin)
            .dataMaskInfo(policyItemDataMaskInfo)
            .build();
      }

      @JsonProperty
      public abstract ImmutableList<PolicyItemAccess> accesses();

      @JsonProperty
      @Nullable
      public abstract ImmutableList<String> users();

      @JsonProperty
      @Nullable
      public abstract ImmutableList<String> groups();

      @JsonProperty
      @Nullable
      public abstract ImmutableList<String> roles();

      @JsonProperty
      @Nullable
      public abstract ImmutableList<ItemCondition> conditions();

      @JsonProperty
      @Nullable
      public abstract Boolean delegateAdmin();

      @JsonProperty
      @Nullable
      public abstract PolicyItemDataMaskInfo dataMaskInfo();
    }

    @AutoValue
    @JsonSerialize(as = RowFilterPolicyItem.class)
    public abstract static class RowFilterPolicyItem {

      @AutoValue
      @JsonSerialize(as = PolicyItemRowFilterInfo.class)
      public abstract static class PolicyItemRowFilterInfo {

        @AutoValue.Builder
        public abstract static class Builder {

          public abstract Builder filterExpr(String value);

          public abstract PolicyItemRowFilterInfo build();
        }

        public static Builder builder() {
          return new AutoValue_RangerDumpFormat_Policy_RowFilterPolicyItem_PolicyItemRowFilterInfo
              .Builder();
        }

        @JsonCreator
        public static PolicyItemRowFilterInfo create(
            @JsonProperty("filterExpr") String filterExpr) {
          return builder().filterExpr(filterExpr).build();
        }

        @JsonProperty
        public abstract String filterExpr();
      }

      @AutoValue.Builder
      public abstract static class Builder {

        public abstract Builder accesses(List<PolicyItemAccess> value);

        public abstract Builder users(List<String> value);

        public abstract Builder groups(List<String> value);

        public abstract Builder roles(List<String> value);

        public abstract Builder conditions(List<ItemCondition> value);

        public abstract Builder delegateAdmin(Boolean value);

        public abstract Builder rowFilterInfo(PolicyItemRowFilterInfo value);

        public abstract RowFilterPolicyItem build();
      }

      public static Builder builder() {
        return new AutoValue_RangerDumpFormat_Policy_RowFilterPolicyItem.Builder();
      }

      @JsonCreator
      public static RowFilterPolicyItem create(
          @JsonProperty("accesses") ImmutableList<PolicyItemAccess> accesses,
          @JsonProperty("users") ImmutableList<String> users,
          @JsonProperty("groups") ImmutableList<String> groups,
          @JsonProperty("roles") ImmutableList<String> roles,
          @JsonProperty("conditions") ImmutableList<ItemCondition> conditions,
          @JsonProperty("delegateAdmin") Boolean delegateAdmin,
          @JsonProperty("rowFilterItem") PolicyItemRowFilterInfo rowFilterInfo) {
        return builder()
            .accesses(accesses)
            .users(users)
            .groups(groups)
            .roles(roles)
            .conditions(conditions)
            .delegateAdmin(delegateAdmin)
            .rowFilterInfo(rowFilterInfo)
            .build();
      }

      @JsonProperty
      public abstract ImmutableList<PolicyItemAccess> accesses();

      @JsonProperty
      @Nullable
      public abstract ImmutableList<String> users();

      @JsonProperty
      @Nullable
      public abstract ImmutableList<String> groups();

      @JsonProperty
      @Nullable
      public abstract ImmutableList<String> roles();

      @JsonProperty
      @Nullable
      public abstract ImmutableList<ItemCondition> conditions();

      @JsonProperty
      @Nullable
      public abstract Boolean delegateAdmin();

      @JsonProperty
      @Nullable
      public abstract PolicyItemRowFilterInfo rowFilterInfo();
    }

    @AutoValue
    @JsonSerialize(as = ValiditySchedule.class)
    public abstract static class ValiditySchedule {

      @AutoValue
      @JsonSerialize(as = ValidityRecurrence.class)
      public abstract static class ValidityRecurrence {

        @AutoValue
        @JsonSerialize(as = ValidityRecurrence.class)
        public abstract static class RecurrenceSchedule {

          @AutoValue.Builder
          public abstract static class Builder {

            public abstract Builder minute(String value);

            public abstract Builder hour(String value);

            public abstract Builder dayOfMonth(String value);

            public abstract Builder dayOfWeek(String value);

            public abstract Builder month(String value);

            public abstract Builder year(String value);

            public abstract RecurrenceSchedule build();
          }

          public static Builder builder() {
            return new AutoValue_RangerDumpFormat_Policy_ValiditySchedule_ValidityRecurrence_RecurrenceSchedule
                .Builder();
          }

          @JsonCreator
          public static RecurrenceSchedule create(
              @JsonProperty("minute") String minute,
              @JsonProperty("hour") String hour,
              @JsonProperty("dayOfMonth") String dayOfMonth,
              @JsonProperty("dayOfWeek") String dayOfWeek,
              @JsonProperty("month") String month,
              @JsonProperty("year") String year) {
            return builder()
                .minute(minute)
                .hour(hour)
                .dayOfMonth(dayOfMonth)
                .dayOfWeek(dayOfWeek)
                .month(month)
                .year(year)
                .build();
          }

          @JsonProperty
          @Nullable
          public abstract String minute();

          @JsonProperty
          @Nullable
          public abstract String hour();

          @JsonProperty
          @Nullable
          public abstract String dayOfMonth();

          @JsonProperty
          @Nullable
          public abstract String dayOfWeek();

          @JsonProperty
          @Nullable
          public abstract String month();

          @JsonProperty
          @Nullable
          public abstract String year();
        }

        @AutoValue
        @JsonSerialize(as = ValidityInterval.class)
        public abstract static class ValidityInterval {

          @AutoValue.Builder
          public abstract static class Builder {

            public abstract Builder days(Integer value);

            public abstract Builder hours(Integer value);

            public abstract Builder minutes(Integer value);

            public abstract ValidityInterval build();
          }

          public static Builder builder() {
            return new AutoValue_RangerDumpFormat_Policy_ValiditySchedule_ValidityRecurrence_ValidityInterval
                .Builder();
          }

          @JsonCreator
          public static ValidityInterval create(
              @JsonProperty("days") Integer days,
              @JsonProperty("hours") Integer hours,
              @JsonProperty("minutes") Integer minutes) {
            return builder().days(days).hours(hours).minutes(minutes).build();
          }

          @JsonProperty
          @Nullable
          public abstract Integer days();

          @JsonProperty
          @Nullable
          public abstract Integer hours();

          @JsonProperty
          @Nullable
          public abstract Integer minutes();
        }

        @AutoValue.Builder
        public abstract static class Builder {

          public abstract Builder schedule(RecurrenceSchedule value);

          public abstract Builder interval(ValidityInterval value);

          public abstract ValidityRecurrence build();
        }

        public static Builder builder() {
          return new AutoValue_RangerDumpFormat_Policy_ValiditySchedule_ValidityRecurrence
              .Builder();
        }

        @JsonCreator
        public static ValidityRecurrence create(
            @JsonProperty("schedule") RecurrenceSchedule schedule,
            @JsonProperty("interval") ValidityInterval interval) {
          return builder().schedule(schedule).interval(interval).build();
        }

        @JsonProperty
        @Nullable
        public abstract RecurrenceSchedule schedule();

        @JsonProperty
        @Nullable
        public abstract ValidityInterval interval();
      }

      @AutoValue.Builder
      public abstract static class Builder {

        public abstract Builder startTime(String value);

        public abstract Builder endTime(String value);

        public abstract Builder timeZone(String value);

        public abstract Builder recurrences(List<ValidityRecurrence> value);

        public abstract ValiditySchedule build();
      }

      public static Builder builder() {
        return new AutoValue_RangerDumpFormat_Policy_ValiditySchedule.Builder();
      }

      @JsonCreator
      public static ValiditySchedule create(
          @JsonProperty("startTime") String startTime,
          @JsonProperty("endTime") String endTime,
          @JsonProperty("timeZone") String timeZone,
          @JsonProperty("recurrences") ImmutableList<ValidityRecurrence> recurrences) {
        return builder()
            .startTime(startTime)
            .endTime(endTime)
            .timeZone(timeZone)
            .recurrences(recurrences)
            .build();
      }

      @JsonProperty
      @Nullable
      public abstract String startTime();

      @JsonProperty
      @Nullable
      public abstract String endTime();

      @JsonProperty
      @Nullable
      public abstract String timeZone();

      @JsonProperty
      @Nullable
      public abstract ImmutableList<ValidityRecurrence> recurrences();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder id(long value);

      public abstract Builder guid(String value);

      public abstract Builder isEnabled(Boolean value);

      public abstract Builder createdBy(String value);

      public abstract Builder updatedBy(String value);

      public abstract Builder createDate(Instant value);

      public abstract Builder updateDate(Instant value);

      public abstract Builder version(Long value);

      public abstract Builder service(String value);

      public abstract Builder name(String value);

      public abstract Builder policyType(Integer value);

      public abstract Builder policyPriority(Integer value);

      public abstract Builder description(String value);

      public abstract Builder resourceSignature(String value);

      public abstract Builder isAuditEnabled(Boolean value);

      public abstract Builder resources(Map<String, PolicyResource> value);

      public abstract Builder additionalResources(List<ImmutableMap<String, PolicyResource>> value);

      public abstract Builder conditions(List<ItemCondition> value);

      public abstract Builder policyItems(List<PolicyItem> value);

      public abstract Builder denyPolicyItems(List<PolicyItem> value);

      public abstract Builder allowExceptions(List<PolicyItem> value);

      public abstract Builder denyExceptions(List<PolicyItem> value);

      public abstract Builder dataMaskPolicyItems(List<DataMaskPolicyItem> value);

      public abstract Builder rowFilterPolicyItems(List<RowFilterPolicyItem> value);

      public abstract Builder serviceType(String value);

      public abstract Builder options(Map<String, Object> value);

      public abstract Builder validitySchedules(List<ValiditySchedule> value);

      public abstract Builder policyLabels(List<String> value);

      public abstract Builder zoneName(String value);

      public abstract Builder isDenyAllElse(Boolean value);

      public abstract Policy build();
    }

    public static Builder builder() {
      return new AutoValue_RangerDumpFormat_Policy.Builder();
    }

    @JsonCreator
    public static Policy create(
        @JsonProperty("id") long id,
        @JsonProperty("guid") String guid,
        @JsonProperty("isEnabled") Boolean isEnabled,
        @JsonProperty("createdBy") String createdBy,
        @JsonProperty("updatedBy") String updatedBy,
        @JsonProperty("createDate") Instant createDate,
        @JsonProperty("updateDate") Instant updateDate,
        @JsonProperty("version") Long version,
        @JsonProperty("service") String service,
        @JsonProperty("name") String name,
        @JsonProperty("policyType") Integer policyType,
        @JsonProperty("policyPriority") Integer policyPriority,
        @JsonProperty("description") String description,
        @JsonProperty("resourceSignature") String resourceSignature,
        @JsonProperty("isAuditEnabled") Boolean isAuditEnabled,
        @JsonProperty("resources") ImmutableMap<String, PolicyResource> resources,
        @JsonProperty("additionalResources")
            ImmutableList<ImmutableMap<String, PolicyResource>> additionalResources,
        @JsonProperty("conditions") ImmutableList<ItemCondition> conditions,
        @JsonProperty("policyItems") ImmutableList<PolicyItem> policyItems,
        @JsonProperty("denyPolicyItems") ImmutableList<PolicyItem> denyPolicyItems,
        @JsonProperty("allowExceptions") ImmutableList<PolicyItem> allowExceptions,
        @JsonProperty("denyExceptions") ImmutableList<PolicyItem> denyExceptions,
        @JsonProperty("dataMaskPolicyItems") ImmutableList<DataMaskPolicyItem> dataMaskPolicyItems,
        @JsonProperty("rowFilterPolicyItems")
            ImmutableList<RowFilterPolicyItem> rowFilterPolicyItems,
        @JsonProperty("serviceType") String serviceType,
        @JsonProperty("options") ImmutableMap<String, Object> options,
        @JsonProperty("validitySchedules") ImmutableList<ValiditySchedule> validitySchedules,
        @JsonProperty("policyLabels") ImmutableList<String> policyLabels,
        @JsonProperty("zoneName") String zoneName,
        @JsonProperty("isDenyAllElse") Boolean isDenyAllElse) {
      return builder()
          .id(id)
          .guid(guid)
          .isEnabled(isEnabled)
          .createdBy(createdBy)
          .updatedBy(updatedBy)
          .createDate(createDate)
          .updateDate(updateDate)
          .version(version)
          .service(service)
          .name(name)
          .policyType(policyType)
          .policyPriority(policyPriority)
          .description(description)
          .resourceSignature(resourceSignature)
          .isAuditEnabled(isAuditEnabled)
          .resources(resources)
          .additionalResources(additionalResources)
          .conditions(conditions)
          .policyItems(policyItems)
          .denyPolicyItems(denyPolicyItems)
          .allowExceptions(allowExceptions)
          .denyExceptions(denyExceptions)
          .dataMaskPolicyItems(dataMaskPolicyItems)
          .rowFilterPolicyItems(rowFilterPolicyItems)
          .serviceType(serviceType)
          .options(options)
          .validitySchedules(validitySchedules)
          .policyLabels(policyLabels)
          .zoneName(zoneName)
          .isDenyAllElse(isDenyAllElse)
          .build();
    }

    @JsonProperty
    public abstract long id();

    @JsonProperty
    @Nullable
    public abstract String guid();

    @JsonProperty
    @Nullable
    public abstract Boolean isEnabled();

    @JsonProperty
    @Nullable
    public abstract String createdBy();

    @JsonProperty
    @Nullable
    public abstract String updatedBy();

    @JsonProperty
    @JsonFormat(shape = Shape.STRING)
    @Nullable
    public abstract Instant createDate();

    @JsonProperty
    @JsonFormat(shape = Shape.STRING)
    @Nullable
    public abstract Instant updateDate();

    @JsonProperty
    @Nullable
    public abstract Long version();

    @JsonProperty
    public abstract String service();

    @JsonProperty
    public abstract String name();

    @JsonProperty
    @Nullable
    public abstract Integer policyType();

    @JsonProperty
    @Nullable
    public abstract Integer policyPriority();

    @JsonProperty
    @Nullable
    public abstract String description();

    @JsonProperty
    @Nullable
    public abstract String resourceSignature();

    @JsonProperty
    @Nullable
    public abstract Boolean isAuditEnabled();

    @JsonProperty
    @Nullable
    public abstract ImmutableMap<String, PolicyResource> resources();

    @JsonProperty
    @Nullable
    public abstract ImmutableList<ImmutableMap<String, PolicyResource>> additionalResources();

    @JsonProperty
    @Nullable
    public abstract ImmutableList<ItemCondition> conditions();

    @JsonProperty
    @Nullable
    public abstract ImmutableList<PolicyItem> policyItems();

    @JsonProperty
    @Nullable
    public abstract ImmutableList<PolicyItem> denyPolicyItems();

    @JsonProperty
    @Nullable
    public abstract ImmutableList<PolicyItem> allowExceptions();

    @JsonProperty
    @Nullable
    public abstract ImmutableList<PolicyItem> denyExceptions();

    @JsonProperty
    @Nullable
    public abstract ImmutableList<DataMaskPolicyItem> dataMaskPolicyItems();

    @JsonProperty
    @Nullable
    public abstract ImmutableList<RowFilterPolicyItem> rowFilterPolicyItems();

    @JsonProperty
    @Nullable
    public abstract String serviceType();

    @JsonProperty
    @Nullable
    public abstract ImmutableMap<String, Object> options();

    @JsonProperty
    @Nullable
    public abstract ImmutableList<ValiditySchedule> validitySchedules();

    @JsonProperty
    @Nullable
    public abstract ImmutableList<String> policyLabels();

    @JsonProperty
    @Nullable
    public abstract String zoneName();

    @JsonProperty
    @Nullable
    public abstract Boolean isDenyAllElse();
  }
}
