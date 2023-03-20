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
package com.google.edwmigration.dumper.plugin.lib.dumper.spi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

/** @author shevek */
public interface CoreMetadataDumpFormat {

  public static final YAMLFactory FACTORY =
      new YAMLFactory()
          .enable(YAMLGenerator.Feature.LITERAL_BLOCK_STYLE)
          // .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
          .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
          .disable(YAMLGenerator.Feature.SPLIT_LINES);
  public static final ObjectMapper MAPPER =
      new ObjectMapper(FACTORY)
          // .registerModule(new GuavaModule())
          // .registerModule(new JavaTimeModule())
          // .registerModule(new JodaModule())
          .enable(SerializationFeature.INDENT_OUTPUT)
          .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

  public static interface CompilerWorksDumpMetadataTaskFormat {

    public static final String ZIP_ENTRY_NAME = "compilerworks-metadata.yaml";

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_ABSENT)
    public static class Product {

      public String version;
      public String arguments;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_ABSENT)
    public static class Root {

      public String format;
      /** In milliseconds since the epoch. */
      public long timestamp;

      public Product product;
    }
  }

  interface Group {

    enum Header {
      TASK,
      STATUS
    }
  }
}
