/*
 * Copyright 2022-2024 Google LLC
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
package com.google.edwmigration.dumper.plugin.lib.dumper.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonFormat.Shape;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import javax.annotation.Nullable;

public interface RangerDumpFormat {

  ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
          .enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
          .disable(SerializationFeature.INDENT_OUTPUT);

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
      return new AutoValue_RangerDumpFormat_Service(
          id,
          guid,
          isEnabled,
          createdBy,
          updatedBy,
          createTime,
          updateTime,
          version,
          type,
          name,
          displayName,
          description,
          tagService,
          configs,
          policyVersion,
          policyUpdateTime,
          tagVersion,
          tagUpdateTime);
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
  }

  /**
   * A Ranger user. Based on server's model defined at
   * https://raw.githubusercontent.com/apache/ranger/master/security-admin/src/main/java/org/apache/ranger/view/VXUser.java.
   */
  @AutoValue
  @JsonSerialize(as = User.class)
  abstract class User {

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
      return new AutoValue_RangerDumpFormat_User(
          id,
          createDate,
          updateDate,
          owner,
          updatedBy,
          name,
          firstName,
          lastName,
          emailAddress,
          credStoreId,
          description,
          groupIdList,
          groupNameList,
          status,
          isVisible,
          userSource,
          userRoleList,
          otherAttributes,
          syncSource);
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

      @JsonCreator
      public static RoleMember create(
          @JsonProperty("name") String name, @JsonProperty("isAdmin") boolean isAdmin) {
        return new AutoValue_RangerDumpFormat_Role_RoleMember(name, isAdmin);
      }

      @JsonProperty
      public abstract String name();

      @JsonProperty
      public abstract boolean isAdmin();
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
      return new AutoValue_RangerDumpFormat_Role(
          id,
          guid,
          isEnabled,
          createdBy,
          updatedBy,
          createTime,
          updateTime,
          version,
          name,
          description,
          options,
          users,
          groups,
          roles,
          createdByUser);
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
      return new AutoValue_RangerDumpFormat_Group(
          id,
          createDate,
          updateDate,
          owner,
          updatedBy,
          name,
          description,
          groupType,
          groupSource,
          credStoreId,
          isVisible,
          otherAttributes,
          syncSource);
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

      @JsonCreator
      public static PolicyResource create(
          @JsonProperty("values") ImmutableList<String> values,
          @JsonProperty("isExcludes") Boolean isExcludes,
          @JsonProperty("isRecursive") Boolean isRecursive) {
        return new AutoValue_RangerDumpFormat_Policy_PolicyResource(
            values, isExcludes, isRecursive);
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

      @JsonCreator
      public static ItemCondition create(
          @JsonProperty("type") String type, @JsonProperty("values") ImmutableList<String> values) {
        return new AutoValue_RangerDumpFormat_Policy_ItemCondition(type, values);
      }

      @JsonProperty
      public abstract String type();

      @JsonProperty
      public abstract ImmutableList<String> values();
    }

    @AutoValue
    @JsonSerialize(as = PolicyItemAccess.class)
    public abstract static class PolicyItemAccess {

      @JsonCreator
      public static PolicyItemAccess create(
          @JsonProperty("type") String type, @JsonProperty("isAllowed") Boolean isAllowed) {
        return new AutoValue_RangerDumpFormat_Policy_PolicyItemAccess(type, isAllowed);
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

      @JsonCreator
      public static PolicyItem create(
          @JsonProperty("accesses") ImmutableList<PolicyItemAccess> accesses,
          @JsonProperty("users") ImmutableList<String> users,
          @JsonProperty("groups") ImmutableList<String> groups,
          @JsonProperty("roles") ImmutableList<String> roles,
          @JsonProperty("conditions") ImmutableList<ItemCondition> conditions,
          @JsonProperty("delegateAdmin") Boolean delegateAdmin) {
        return new AutoValue_RangerDumpFormat_Policy_PolicyItem(
            accesses, users, groups, roles, conditions, delegateAdmin);
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

        @JsonCreator
        public static PolicyItemDataMaskInfo create(
            @JsonProperty("dataMaskType") String dataMaskType,
            @JsonProperty("conditionExpr") String conditionExpr,
            @JsonProperty("valueExpr") String valueExpr) {
          return new AutoValue_RangerDumpFormat_Policy_DataMaskPolicyItem_PolicyItemDataMaskInfo(
              dataMaskType, conditionExpr, valueExpr);
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

      @JsonCreator
      public static DataMaskPolicyItem create(
          @JsonProperty("accesses") ImmutableList<PolicyItemAccess> accesses,
          @JsonProperty("users") ImmutableList<String> users,
          @JsonProperty("groups") ImmutableList<String> groups,
          @JsonProperty("roles") ImmutableList<String> roles,
          @JsonProperty("conditions") ImmutableList<ItemCondition> conditions,
          @JsonProperty("delegateAdmin") Boolean delegateAdmin,
          @JsonProperty("dataMaskInfo") PolicyItemDataMaskInfo policyItemDataMaskInfo) {
        return new AutoValue_RangerDumpFormat_Policy_DataMaskPolicyItem(
            accesses, users, groups, roles, conditions, delegateAdmin, policyItemDataMaskInfo);
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

        @JsonCreator
        public static PolicyItemRowFilterInfo create(
            @JsonProperty("filterExpr") String filterExpr) {
          return new AutoValue_RangerDumpFormat_Policy_RowFilterPolicyItem_PolicyItemRowFilterInfo(
              filterExpr);
        }

        @JsonProperty
        public abstract String filterExpr();
      }

      @JsonCreator
      public static RowFilterPolicyItem create(
          @JsonProperty("accesses") ImmutableList<PolicyItemAccess> accesses,
          @JsonProperty("users") ImmutableList<String> users,
          @JsonProperty("groups") ImmutableList<String> groups,
          @JsonProperty("roles") ImmutableList<String> roles,
          @JsonProperty("conditions") ImmutableList<ItemCondition> conditions,
          @JsonProperty("delegateAdmin") Boolean delegateAdmin,
          @JsonProperty("rowFilterItem") PolicyItemRowFilterInfo rowFilterItem) {
        return new AutoValue_RangerDumpFormat_Policy_RowFilterPolicyItem(
            accesses, users, groups, roles, conditions, delegateAdmin, rowFilterItem);
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

          @JsonCreator
          public static RecurrenceSchedule create(
              @JsonProperty("minute") String minute,
              @JsonProperty("hour") String hour,
              @JsonProperty("dayOfMonth") String dayOfMonth,
              @JsonProperty("dayOfWeek") String dayOfWeek,
              @JsonProperty("month") String month,
              @JsonProperty("year") String year) {
            return new AutoValue_RangerDumpFormat_Policy_ValiditySchedule_ValidityRecurrence_RecurrenceSchedule(
                minute, hour, dayOfMonth, dayOfWeek, month, year);
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

          @JsonCreator
          public static ValidityInterval create(
              @JsonProperty("days") Integer days,
              @JsonProperty("hours") Integer hours,
              @JsonProperty("minutes") Integer minutes) {
            return new AutoValue_RangerDumpFormat_Policy_ValiditySchedule_ValidityRecurrence_ValidityInterval(
                days, hours, minutes);
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

        @JsonCreator
        public static ValidityRecurrence create(
            @JsonProperty("schedule") RecurrenceSchedule schedule,
            @JsonProperty("interval") ValidityInterval interval) {
          return new AutoValue_RangerDumpFormat_Policy_ValiditySchedule_ValidityRecurrence(
              schedule, interval);
        }

        @JsonProperty
        @Nullable
        public abstract RecurrenceSchedule schedule();

        @JsonProperty
        @Nullable
        public abstract ValidityInterval interval();
      }

      @JsonCreator
      public static ValiditySchedule create(
          @JsonProperty("startTime") String startTime,
          @JsonProperty("endTime") String endTime,
          @JsonProperty("timeZone") String timeZone,
          @JsonProperty("recurrences") ImmutableList<ValidityRecurrence> recurrences) {
        return new AutoValue_RangerDumpFormat_Policy_ValiditySchedule(
            startTime, endTime, timeZone, recurrences);
      }

      @JsonProperty("startTime")
      @Nullable
      public abstract String startTime();

      @JsonProperty("endTime")
      @Nullable
      public abstract String endTime();

      @JsonProperty("timeZone")
      @Nullable
      public abstract String timeZone();

      @JsonProperty("recurrences")
      @Nullable
      public abstract ImmutableList<ValidityRecurrence> recurrences();
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
      return new AutoValue_RangerDumpFormat_Policy(
          id,
          guid,
          isEnabled,
          createdBy,
          updatedBy,
          createDate,
          updateDate,
          version,
          service,
          name,
          policyType,
          policyPriority,
          description,
          resourceSignature,
          isAuditEnabled,
          resources,
          additionalResources,
          conditions,
          policyItems,
          denyPolicyItems,
          allowExceptions,
          denyExceptions,
          dataMaskPolicyItems,
          rowFilterPolicyItems,
          serviceType,
          options,
          validitySchedules,
          policyLabels,
          zoneName,
          isDenyAllElse);
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
