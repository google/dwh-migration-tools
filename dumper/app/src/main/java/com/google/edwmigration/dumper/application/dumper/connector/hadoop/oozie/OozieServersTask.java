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
package com.google.edwmigration.dumper.application.dumper.connector.hadoop.oozie;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.oozie.client.XOozieClient;

public class OozieServersTask extends AbstractTask<Void> {

  public OozieServersTask() {
    super("oozie_servers.csv");
  }

  @CheckForNull
  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    final CSVFormat csvFormat = FORMAT.withHeader(CSVHeader.class);
    try (CSVPrinter printer = csvFormat.print(sink.asCharSink(UTF_8).openBufferedStream())) {
      XOozieClient oozieClient = ((OozieHandle) handle).getOozieClient();
      Map<String, String> servers = oozieClient.getAvailableOozieServers();
      for (Entry<String, String> entry : servers.entrySet()) {
        printer.printRecord(entry.getKey(), entry.getValue());
      }
    }
    return null;
  }

  enum CSVHeader {
    Server,
    URL
  }
}
