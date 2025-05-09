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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClouderaManagerHandleTest {

  @Mock private CloseableHttpClient httpClient;
  private final URI localhost = URI.create("http://localhost");

  @Test
  public void apiUrl_normalized_success() {
    // trailing slash
    URI apiURI = URI.create("https://localhost/some//api/path/with/trailing/slash////");
    ClouderaManagerHandle handle = new ClouderaManagerHandle(apiURI, httpClient);

    assertEquals("https://localhost/", handle.getBaseURI().toString());
    assertEquals(
        URI.create("https://localhost/some/api/path/with/trailing/slash/"), handle.getApiURI());
  }

  @Test
  public void apiUrl_queryAndFragmentsRemoved_success() {
    // with some path and query params
    URI apiURI = URI.create("https://localhost:1234/x/y/z?q=42");
    ClouderaManagerHandle handle = new ClouderaManagerHandle(apiURI, httpClient);

    assertEquals("https://localhost:1234/", handle.getBaseURI().toString());
    assertEquals("https://localhost:1234/x/y/z/", handle.getApiURI().toString());

    // with some path and query params and fragments
    apiURI = URI.create("https://localhost:1234/x/y/z?q=42#some-place");
    handle = new ClouderaManagerHandle(apiURI, httpClient);

    assertEquals("https://localhost:1234/", handle.getBaseURI().toString());
    assertEquals("https://localhost:1234/x/y/z/", handle.getApiURI().toString());
  }

  @Test
  public void apiUrl_resolved_success() {
    // host with port
    URI apiURI = URI.create("https://localhost:1234");
    ClouderaManagerHandle handle = new ClouderaManagerHandle(apiURI, httpClient);

    assertEquals("https://localhost:1234/", handle.getBaseURI().toString());
    // trailing slash added
    assertEquals("https://localhost:1234/", handle.getApiURI().toString());

    // without port
    apiURI = URI.create("https://localhost/some/api/path");
    handle = new ClouderaManagerHandle(apiURI, httpClient);

    assertEquals("https://localhost/", handle.getBaseURI().toString());
    assertEquals("https://localhost/some/api/path/", handle.getApiURI().toString());
  }

  @Test
  public void initClusters_success() {
    ClouderaManagerHandle handle = new ClouderaManagerHandle(localhost, httpClient);

    List<ClouderaClusterDTO> dtos = new ArrayList<>();
    dtos.add(ClouderaClusterDTO.create("1", "first"));
    dtos.add(ClouderaClusterDTO.create("2", "second"));

    handle.initClusters(dtos);

    assertEquals(dtos, handle.getClusters());
  }

  @Test
  public void initClusterWithEmptyList_throwsException() {
    ClouderaManagerHandle handle = new ClouderaManagerHandle(localhost, httpClient);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> handle.initClusters(ImmutableList.of()));

    assertEquals("Clusters can't be initialised to empty list.", exception.getMessage());

    NullPointerException npe =
        assertThrows(NullPointerException.class, () -> handle.initClusters(null));

    assertEquals("Clusters can't be initialised to null list.", npe.getMessage());
  }

  @Test
  public void initClustersTwice_throwsException() {
    ClouderaManagerHandle handle = new ClouderaManagerHandle(localhost, httpClient);

    List<ClouderaClusterDTO> first =
        ImmutableList.of(
            ClouderaClusterDTO.create("1", "first"), ClouderaClusterDTO.create("2", "second"));

    List<ClouderaClusterDTO> second =
        ImmutableList.of(
            ClouderaClusterDTO.create("I", "prime"), ClouderaClusterDTO.create("II", "backup"));

    handle.initClusters(first);

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> handle.initClusters(second));

    assertEquals(first, handle.getClusters());
    assertEquals("The cluster already initialized!", exception.getMessage());
  }

  // @Test todo
  public void initHostsWithNullOrEmpty_throwsException() {
    ClouderaManagerHandle handle = new ClouderaManagerHandle(localhost, httpClient);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> handle.initHostsIfNull(ImmutableList.of()));

    assertEquals("Hosts can't be initialised to empty list.", exception.getMessage());

    NullPointerException npe =
        assertThrows(NullPointerException.class, () -> handle.initHostsIfNull(null));

    assertEquals("Hosts can't be initialised to null list.", npe.getMessage());
  }
}
