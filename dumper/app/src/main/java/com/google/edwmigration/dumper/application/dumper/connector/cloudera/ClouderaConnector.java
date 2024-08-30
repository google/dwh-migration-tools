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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentAssessment;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.hdfs.HdfsExtractionConnector;
import com.google.edwmigration.dumper.application.dumper.connector.hive.HiveMetadataConnector;
import com.google.edwmigration.dumper.application.dumper.connector.meta.AbstractMetaConnector;
import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerConnector;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import javax.annotation.Nonnull;

@AutoService(Connector.class)
@Description("Dumps metadata from the Cloudera (Hadoop on-prem) cluster.")
@RespectsArgumentAssessment
public class ClouderaConnector extends AbstractMetaConnector {

  public ClouderaConnector() {
    super(
        "cloudera",
        "cloudera.zip",
        ImmutableList.of(
            ClouderaMetadataConnector.NAME,
            HiveMetadataConnector.NAME,
            HdfsExtractionConnector.NAME,
            RangerConnector.NAME));
  }

  @Nonnull
  @Override
  public Class<? extends Enum<? extends ConnectorProperty>> getConnectorProperties() {
    return ClouderaConnectorProperty.class;
  }

  public enum ClouderaConnectorProperty implements ConnectorProperty {
    USER("ranger.user", "Ranger API username"),
    PASSWORD("ranger.password", "Ranger API password");

    private final String name;
    private final String description;

    ClouderaConnectorProperty(String name, String description) {
      this.name = name;
      this.description = description;
    }

    @Nonnull
    public String getName() {
      return name;
    }

    @Nonnull
    public String getDescription() {
      return description;
    }
  }
}
