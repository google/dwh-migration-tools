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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Ascii;
import com.google.common.base.Joiner;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.representer.Representer;
import org.yaml.snakeyaml.resolver.Resolver;

/** Converter from YAML representation to protobuf representation. */
public final class YamlToProtoConverter {
  private static class CustomResolver extends Resolver {
    @Override
    protected void addImplicitResolvers() {
      // Everything resolves to a string. The proto provides the schema
    }
  }

  private static final String GET_DEFAULT_INSTANCE_MSG =
      "Message class must implement the #getDefaultInstance() static method: ";

  public static <T extends MessageLite> T getDefaultInstance(Class<T> type) {
    try {
      return type.cast(type.getMethod("getDefaultInstance").invoke(null));
    } catch (ReflectiveOperationException | ClassCastException e) {
      throw new IllegalArgumentException(GET_DEFAULT_INSTANCE_MSG + type, e);
    }
  }

  /**
   * Converts a YAML string to a built protobuf Message of the given class.
   *
   * @param clazz the Message protobuf class to build
   * @param textProto the strings to convert
   * @return the built protobuf
   * @throws YamlConversionException if there was a problem converting the Yaml using the specified
   *     Message type.
   */
  @SuppressWarnings("unchecked")
  public static <T extends Message> T yamlToMessage(Class<T> clazz, String... textProto) {
    Message.Builder builder = getDefaultInstance(clazz).newBuilderForType();
    String textProtoJoined = Joiner.on('\n').join(textProto);
    yamlToMessage(new ByteArrayInputStream(textProtoJoined.getBytes(UTF_8)), builder);
    return (T) builder.build();
  }

  /**
   * Converts a YAML input stream to a protobuf Message.
   *
   * @param stream the input stream for a YAML document
   * @param builder the Message builder to convert to
   * @throws YamlConversionException if there was a problem converting the Yaml using the specified
   *     Message type.
   */
  public static void yamlToMessage(InputStream stream, Message.Builder builder) {
    LoaderOptions loaderOptions = new LoaderOptions();
    loaderOptions.setAllowDuplicateKeys(false);
    Yaml yaml =
        new Yaml(
            new SafeConstructor(new LoaderOptions()),
            new Representer(new DumperOptions()),
            new DumperOptions(),
            loaderOptions,
            new CustomResolver());
    Object obj;
    try {
      obj = yaml.load(stream);
    } catch (YAMLException ise) { // unfortunately YAMLException is a RuntimeException
      throw YamlConversionException.formatMessage(
          ise, "Failed to parse YAML: %s", ise.getMessage());
    }
    yamlObjectToMessage(Optional.empty(), obj, builder);
  }

  private static void yamlObjectToMessage(
      Optional<FieldDescriptor> referencingDescriptor, Object yamlObj, Message.Builder builder) {
    Map<?, ?> map = cast(referencingDescriptor, yamlObj, Map.class);
    Descriptor descriptor = builder.getDescriptorForType();
    map.forEach(
        (key, value) -> {
          FieldDescriptor fieldDescriptor = descriptor.findFieldByName(key.toString());
          if (fieldDescriptor == null) {
            throw YamlConversionException.formatMessage(
                "No field %s defined for message %s", key, descriptor.getFullName());
          }
          yamlObjectToField(value, fieldDescriptor, builder, /* isRepeatedContext= */ false);
        });
  }

  private static void yamlObjectToField(
      Object yamlObj,
      FieldDescriptor fieldDescriptor,
      Message.Builder builder,
      boolean isRepeatedContext) {
    OneofDescriptor oneof = fieldDescriptor.getContainingOneof();
    if (oneof != null && builder.hasOneof(oneof)) {
      throw YamlConversionException.formatMessage(
          "Attempting to write message `%s` to a builder that already has another oneof value:"
              + " %s",
          yamlObj, builder);
    }
    if (fieldDescriptor.isMapField()) {
      yamlObjectToMapField(yamlObj, fieldDescriptor, builder);
      return;
    }
    if (!isRepeatedContext && fieldDescriptor.isRepeated()) {
      yamlObjectToRepeatedField(yamlObj, fieldDescriptor, builder);
      return;
    }

    Object value;
    if (fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
      Message.Builder fieldBuilder = builder.newBuilderForField(fieldDescriptor);
      yamlObjectToMessage(Optional.of(fieldDescriptor), yamlObj, fieldBuilder);
      value = fieldBuilder.build();
    } else {
      String strVal = cast(Optional.of(fieldDescriptor), yamlObj, String.class);
      try {
        switch (fieldDescriptor.getJavaType()) {
          case BOOLEAN:
            switch (strVal) {
              case "false":
                value = false;
                break;
              case "true":
                value = true;
                break;
              default:
                throw YamlConversionException.formatMessage(
                    "field %s of type boolean assigned %s, does not have value true or false",
                    fieldDescriptor, strVal);
            }
            break;
          case DOUBLE:
            value = Double.valueOf(strVal);
            break;
          case ENUM:
            value = fieldDescriptor.getEnumType().findValueByName(Ascii.toUpperCase(strVal));
            if (value == null) {
              throw YamlConversionException.formatMessage(
                  "Cannot convert %s to enum value in %s", strVal, fieldDescriptor);
            }
            break;
          case FLOAT:
            value = Float.valueOf(strVal);
            break;
          case INT:
            value = Integer.valueOf(strVal);
            break;
          case LONG:
            value = Long.valueOf(strVal);
            break;
          case STRING:
            value = strVal;
            break;
          default:
            throw YamlConversionException.formatMessage(
                "Don't know how to parse type %s for field %s",
                fieldDescriptor.getJavaType(), fieldDescriptor);
        }
      } catch (NumberFormatException e) {
        throw YamlConversionException.formatMessage(
            e,
            "Could not convert %s to a %s for field %s",
            strVal,
            fieldDescriptor.getJavaType(),
            fieldDescriptor);
      }
    }
    if (!isRepeatedContext) {
      builder.setField(fieldDescriptor, value);
    } else {
      builder.addRepeatedField(fieldDescriptor, value);
    }
  }

  private static void yamlObjectToRepeatedField(
      Object yamlObj, FieldDescriptor fieldDescriptor, Message.Builder builder) {
    List<?> list = cast(Optional.of(fieldDescriptor), yamlObj, List.class);
    for (Object entry : list) {
      yamlObjectToField(entry, fieldDescriptor, builder, /* isRepeatedContext= */ true);
    }
  }

  private static void yamlObjectToMapField(
      Object yamlObj, FieldDescriptor fieldDescriptor, Message.Builder builder) {
    Map<?, ?> map = cast(Optional.of(fieldDescriptor), yamlObj, Map.class);
    for (Object key : map.keySet()) {
      Message.Builder mapBuilder = builder.newBuilderForField(fieldDescriptor);
      Object value = map.get(key);
      Descriptor mapDescriptor = mapBuilder.getDescriptorForType();
      FieldDescriptor keyDescriptor = mapDescriptor.findFieldByName("key");
      FieldDescriptor valueDescriptor = mapDescriptor.findFieldByName("value");
      yamlObjectToField(key, keyDescriptor, mapBuilder, /* isRepeatedContext= */ false);
      yamlObjectToField(value, valueDescriptor, mapBuilder, /* isRepeatedContext= */ false);
      builder.addRepeatedField(fieldDescriptor, mapBuilder.build());
    }
  }

  private static <T> T cast(Optional<FieldDescriptor> descriptor, Object obj, Class<T> klass) {
    try {
      return klass.cast(obj);
    } catch (ClassCastException e) {
      throw YamlConversionException.formatMessage(
          e,
          "%s is of type %s, not %s",
          (descriptor.isPresent() ? descriptor.get() : "<top-level>"),
          obj.getClass(),
          klass);
    }
  }

  private YamlToProtoConverter() {}
}
