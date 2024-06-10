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
package com.google.edwmigration.dumper.application.dumper.connector.hive;

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
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
   * Writes the list of Thrift objects as a JSON array to the JsonWriter. The method is not
   * thread-safe.
   */
  public void serialize(List<? extends TBase<?, ?>> thriftObjects, JsonGenerator jsonGenerator)
      throws IOException, TException {
    jsonGenerator.writeStartArray();
    for (TBase<?, ?> thriftObject : thriftObjects) {
      jsonGenerator.writeRawValue(underlyingSerializer.toString(thriftObject));
    }
    jsonGenerator.writeEndArray();
  }
}
