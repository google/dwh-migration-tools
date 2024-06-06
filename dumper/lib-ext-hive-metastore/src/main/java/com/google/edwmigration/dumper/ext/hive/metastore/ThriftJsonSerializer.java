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
package com.google.edwmigration.dumper.ext.hive.metastore;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.transport.TTransportException;

public class ThriftJsonSerializer {
  private final TSerializer underlyingSerializer;

  public ThriftJsonSerializer() throws TTransportException {
    this.underlyingSerializer = new TSerializer(new TSimpleJSONProtocol.Factory());
  }

  public String serialize(TBase<?, ?> thriftObject) throws TException {
    return underlyingSerializer.toString(thriftObject);
  }

  /**
   * Writes the list of Thrift objects as a JSON array to the Writer. The method is not thread-safe.
   */
  public void serialize(List<? extends TBase<?, ?>> thriftObjects, Writer writer)
      throws IOException, TException {
    writer.write('[');
    boolean first = true;
    for (TBase<?, ?> thriftObject : thriftObjects) {
      if (first) {
        first = false;
      } else {
        writer.write(',');
      }
      writer.write(underlyingSerializer.toString(thriftObject));
    }
    writer.write(']');
  }
}
