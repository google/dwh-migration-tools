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
import java.time.Instant;
import java.util.List;
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
        @JsonProperty("groupIdList") List<Long> groupIdList,
        @JsonProperty("groupNameList") List<String> groupNameList,
        @JsonProperty("status") Integer status,
        @JsonProperty("isVisible") Integer isVisible,
        @JsonProperty("userSource") Integer userSource,
        @JsonProperty("userRoleList") List<String> userRoleList,
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
    public abstract List<Long> groupIdList();

    @JsonProperty
    public abstract List<String> groupNameList();

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
    public abstract List<String> userRoleList();

    @JsonProperty
    @Nullable
    public abstract String otherAttributes();

    @JsonProperty
    @Nullable
    public abstract String syncSource();
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
}
