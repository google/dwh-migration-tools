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

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

import com.google.common.io.Resources;
import com.google.protobuf.TextFormat;
import java.io.IOException;
import java.io.InputStream;
import org.junit.jupiter.api.Test;

public class YamlToProtoConverterTest {

  @Test
  public void yamlToMessage_testInputStream_success() throws IOException {
    YamlTest.NestedType.Builder actualBuilder = YamlTest.NestedType.newBuilder();

    try (InputStream yamlStream =
        Resources.asByteSource(Resources.getResource("test.yaml")).openStream()) {
      YamlToProtoConverter.yamlToMessage(yamlStream, actualBuilder);
    }

    YamlTest.NestedType expected = readTextpb();
    assertThat(actualBuilder.build()).isEqualTo(expected);
  }

  @Test
  public void yamlToMessage_testString_success() throws IOException {
    String yamlString = Resources.toString(Resources.getResource("test.yaml"), UTF_8);

    YamlTest.NestedType actual =
        YamlToProtoConverter.yamlToMessage(YamlTest.NestedType.class, yamlString);

    YamlTest.NestedType expected = readTextpb();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void yamlToMessage_duplicateKeys_throwsException() throws IOException {
    YamlTest.NestedType.Builder actualBuilder = YamlTest.NestedType.newBuilder();
    try (InputStream yamlStream =
        Resources.asByteSource(Resources.getResource("bad.yaml")).openStream()) {
      assertThrows(
          "duplicate key int32_type",
          YamlConversionException.class,
          () -> YamlToProtoConverter.yamlToMessage(yamlStream, actualBuilder));
    }
  }

  private YamlTest.NestedType readTextpb() throws IOException {
    return TextFormat.parse(
        Resources.toString(Resources.getResource("test.textpb"), UTF_8), YamlTest.NestedType.class);
  }
}
