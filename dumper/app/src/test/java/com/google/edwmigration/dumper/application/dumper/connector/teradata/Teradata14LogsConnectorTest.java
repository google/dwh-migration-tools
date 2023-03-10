/*
 * Copyright 2022 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnectorTest;
import java.io.IOException;
import java.util.ArrayList;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 * @author shevek
 */
@RunWith(JUnit4.class)
public class Teradata14LogsConnectorTest extends AbstractConnectorTest {

    private final Teradata14LogsConnector connector = new Teradata14LogsConnector();

    @Test
    public void testConnector() throws Exception {
        testConnectorDefaults(connector);
    }

    @Test
    public void testFailsForInvalidQueryLogTimespan() throws IOException {
        ConnectorArguments arguments = new ConnectorArguments(new String[] {
            "--query-log-days", "0",
            "--connector", "snowflake"
        });
        MetadataDumperUsageException exception =
        Assert.assertThrows(
            MetadataDumperUsageException.class,
            () -> connector.addTasksTo(new ArrayList<>(), arguments));

        Assert.assertTrue(exception.getMessage().startsWith("At least one day of query logs should be exported"));

    }

}
