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
package com.google.edwmigration.permissions;

import com.google.cloud.Identity;
import com.google.cloud.Policy;
import com.google.cloud.Role;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.edwmigration.permissions.models.TableIdParser;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles calls to BigQuery tables API. */
public class BqTableClient implements IamClient {

  private static final Logger LOG = LoggerFactory.getLogger(BqTableClient.class);

  private final BigQuery bigQuery;

  private BqTableClient(BigQuery bigQuery) {
    this.bigQuery = bigQuery;
  }

  public static BqTableClient create() throws IOException {
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    return new BqTableClient(bigquery);
  }

  public void addIamPolicyBindings(
      String bqPath, Map<Role, Set<Identity>> bindings, ExtraPermissions extraPermissions) {
    TableId tableId = TableIdParser.parseTranslationId(bqPath);
    Policy.Builder policyBuilder =
        extraPermissions == ExtraPermissions.KEEP
            ? bigQuery.getIamPolicy(tableId).toBuilder()
            : createEmptyPolicyBuilder();
    bindings.forEach(
        (role, identities) ->
            identities.forEach((identity) -> policyBuilder.addIdentity(role, identity)));
    LOG.info("Apply policy {} to table {}", bindings, tableId.getIAMResourceName());
    bigQuery.setIamPolicy(tableId, policyBuilder.build());
  }

  private Policy.Builder createEmptyPolicyBuilder() {
    return Policy.newBuilder().setVersion(1);
  }

  @Override
  public void close() {}
}
