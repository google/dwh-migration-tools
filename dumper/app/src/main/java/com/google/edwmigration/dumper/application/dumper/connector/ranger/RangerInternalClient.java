/*
 * Copyright 2022-2023 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.connector.ranger;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.http.HttpHeaders.ACCEPT;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonFormat.Shape;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.http.HttpEntity;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.ranger.plugin.model.RangerRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A client to pull data from non-public Ranger API that are not covered by the official client. */
public class RangerInternalClient {

  private static final Logger LOG = LoggerFactory.getLogger(RangerConnector.class);

  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  /**
   * A Ranger user. Based on server's model defined at
   * https://raw.githubusercontent.com/apache/ranger/master/security-admin/src/main/java/org/apache/ranger/view/VXUser.java.
   */
  @AutoValue
  @JsonSerialize(as = User.class)
  public abstract static class User {

    @JsonCreator
    static User create(
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
      return new AutoValue_RangerInternalClient_User(
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
    abstract long id();

    @JsonProperty
    @JsonFormat(shape = Shape.STRING)
    @Nullable
    abstract Instant createDate();

    @JsonProperty
    @JsonFormat(shape = Shape.STRING)
    @Nullable
    abstract Instant updateDate();

    @JsonProperty
    @Nullable
    abstract String owner();

    @JsonProperty
    @Nullable
    abstract String updatedBy();

    @JsonProperty
    abstract String name();

    @JsonProperty
    @Nullable
    abstract String firstName();

    @JsonProperty
    @Nullable
    abstract String lastName();

    @JsonProperty
    @Nullable
    abstract String emailAddress();

    @JsonProperty
    @Nullable
    abstract String credStoreId();

    @JsonProperty
    @Nullable
    abstract String description();

    @JsonProperty
    abstract List<Long> groupIdList();

    @JsonProperty
    abstract List<String> groupNameList();

    @JsonProperty
    @Nullable
    abstract Integer status();

    @JsonProperty
    @Nullable
    abstract Integer isVisible();

    @JsonProperty
    @Nullable
    abstract Integer userSource();

    @JsonProperty
    abstract List<String> userRoleList();

    @JsonProperty
    @Nullable
    abstract String otherAttributes();

    @JsonProperty
    @Nullable
    abstract String syncSource();
  }

  @AutoValue
  abstract static class ListUsersResponse {

    @JsonCreator
    static ListUsersResponse create(@JsonProperty("vXUsers") List<User> vXUsers) {
      return new AutoValue_RangerInternalClient_ListUsersResponse(vXUsers);
    }

    @JsonProperty
    abstract List<User> vXUsers();
  }

  /**
   * A Ranger group. Based on server's model defined at
   * https://raw.githubusercontent.com/apache/ranger/master/security-admin/src/main/java/org/apache/ranger/view/VXGroup.java.
   */
  @AutoValue
  @JsonSerialize(as = Group.class)
  public abstract static class Group {

    @JsonCreator
    static Group create(
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
      return new AutoValue_RangerInternalClient_Group(
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
    abstract long id();

    @JsonProperty
    @JsonFormat(shape = Shape.STRING)
    @Nullable
    abstract Instant createDate();

    @JsonProperty
    @JsonFormat(shape = Shape.STRING)
    @Nullable
    abstract Instant updateDate();

    @JsonProperty
    @Nullable
    abstract String owner();

    @JsonProperty
    @Nullable
    abstract String updatedBy();

    @JsonProperty
    abstract String name();

    @JsonProperty
    @Nullable
    abstract String description();

    @JsonProperty
    @Nullable
    abstract Integer groupType();

    @JsonProperty
    @Nullable
    abstract Integer groupSource();

    @JsonProperty
    @Nullable
    abstract String credStoreId();

    @JsonProperty
    @Nullable
    abstract Integer isVisible();

    @JsonProperty
    @Nullable
    abstract String otherAttributes();

    @JsonProperty
    @Nullable
    abstract String syncSource();
  }

  @AutoValue
  abstract static class ListGroupsResponse {

    @JsonCreator
    static ListGroupsResponse create(@JsonProperty("vXGroups") List<Group> vXGroups) {
      return new AutoValue_RangerInternalClient_ListGroupsResponse(vXGroups);
    }

    @JsonProperty
    abstract List<Group> vXGroups();
  }

  @AutoValue
  abstract static class ListRolesResponse {

    @JsonCreator
    static ListRolesResponse create(@JsonProperty("roles") List<RangerRole> roles) {
      return new AutoValue_RangerInternalClient_ListRolesResponse(roles);
    }

    @JsonProperty
    abstract List<RangerRole> roles();
  }

  public static class RangerInternalException extends Exception {

    public RangerInternalException(String message, Throwable cause) {
      super(message, cause);
    }

    public RangerInternalException(String message) {
      super(message);
    }
  }

  private final URI baseUri;

  private final CloseableHttpClient httpClient;

  private final UsernamePasswordCredentials credentials;

  public RangerInternalClient(URI baseUri, String user, String password) throws URISyntaxException {
    this.baseUri = baseUri;
    httpClient = HttpClients.createMinimal();
    credentials = new UsernamePasswordCredentials(user, password);
  }

  public List<User> findUsers(Map<String, String> args) throws RangerInternalException {
    return doHttpGet("/service/xusers/users", args, ListUsersResponse.class).vXUsers();
  }

  public List<Group> findGroups(Map<String, String> args) throws RangerInternalException {
    return doHttpGet("/service/xusers/groups", args, ListGroupsResponse.class).vXGroups();
  }

  public List<RangerRole> findRoles(Map<String, String> args) throws RangerInternalException {
    return doHttpGet("/service/roles/roles", args, ListRolesResponse.class).roles();
  }

  private <T> T doHttpGet(String path, Map<String, String> params, Class<T> bodyClass)
      throws RangerInternalException {
    URI uri;
    try {
      uri =
          new URIBuilder(baseUri + path)
              .addParameters(
                  params.entrySet().stream()
                      .map(entry -> new BasicNameValuePair(entry.getKey(), entry.getValue()))
                      .collect(toImmutableList()))
              .build();
    } catch (URISyntaxException e) {
      throw new RangerInternalException("Failed to build API URI: " + e.getMessage(), e);
    }
    HttpGet httpRequest = new HttpGet(uri);
    httpRequest.addHeader(ACCEPT, APPLICATION_JSON.toString());
    try {
      httpRequest.addHeader(new BasicScheme().authenticate(credentials, httpRequest));
    } catch (AuthenticationException e) {
      throw new RangerInternalException(
          "Failed to initialize authentication header: " + e.getMessage(), e);
    }
    CloseableHttpResponse httpResponse = null;
    try {
      httpResponse = httpClient.execute(httpRequest);
    } catch (IOException e) {
      throw new RangerInternalException(
          "Failed to connect to Ranger internal API: " + e.getMessage(), e);
    }
    LOG.debug("Response from Ranger({}): {}", path, httpResponse);
    if (httpResponse.getStatusLine().getStatusCode() != SC_OK) {
      throw new RangerInternalException(
          "Failed to fetch data from Ranger internal API: "
              + httpResponse.getStatusLine().toString());
    }
    HttpEntity entity = httpResponse.getEntity();
    try {
      return MAPPER.readValue(EntityUtils.toString(entity), bodyClass);
    } catch (IOException e) {
      throw new RangerInternalException(
          "Failed to deserialize Ranger internal API response body: " + e.getMessage(), e);
    }
  }
}
