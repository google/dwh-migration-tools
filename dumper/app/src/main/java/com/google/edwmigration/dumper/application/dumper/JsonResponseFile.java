/*
 * Copyright 2022-2023 Google LLC
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
package com.google.edwmigration.dumper.application.dumper;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts JSON argument-files to command line arguments.
 *
 * <p>The user can give a @file.json which is converted to command line arguments with nested maps
 * becoming --a-b-c and arrays getting converted to ':' seperated values. The output can be given to
 * joptsimple.
 */
public class JsonResponseFile {

  private static final Logger LOG = LoggerFactory.getLogger(JsonResponseFile.class);

  /** Adds arguments from JSON response files to the argument string(s). */
  @Nonnull
  public static String[] addResponseFiles(@Nonnull @NonNull String @NonNull [] args)
      throws IOException {
    List<String> ret = new ArrayList<>();
    for (String argument : args) {
      if (argument.startsWith("@")) {
        ret.addAll(to_arguments(new File(argument.substring(1))));
      } else {
        ret.add(argument);
      }
    }
    return ret.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
  }

  @Nonnull
  private static ObjectMapper newObjectMapper() {
    return new ObjectMapper(new YAMLFactory());
  }

  @Nonnull
  private static List<? extends String> to_arguments(@Nonnull File file) throws IOException {
    return convert(newObjectMapper().readTree(file));
  }

  @VisibleForTesting
  /* pp */ static List<? extends String> to_arguments(@Nonnull String text)
      throws IOException, JsonProcessingException {
    return convert(newObjectMapper().readTree(text));
  }

  @Nonnull
  private static List<? extends String> convert(@Nonnull JsonNode node) {
    List<String> out = new ArrayList<>();
    convert(node, out);
    return out;
  }

  private static void convert(@Nonnull JsonNode node, @Nonnull List<? super String> ret) {
    Preconditions.checkArgument(node.isObject(), "Object expected as root node of JSON file.");
    node.fields()
        .forEachRemaining(
            entry -> {
              String name = entry.getKey();
              JsonNode childNode = entry.getValue();
              if (name.equals("definitions")) convertDefinitions("-D", "", ".", childNode, ret);
              else convertArgument("--" + name, childNode, ret);
            });
  }

  private static void convertDefinitions(
      String s, String s1, String s2, JsonNode childnode, List<? super String> ret) {
    Preconditions.checkArgument(childnode.isObject(), "Definitions should be a JSON object");
    childnode
        .fields()
        .forEachRemaining(
            entry -> {
              Preconditions.checkState(
                  entry.getValue().isTextual(),
                  "Definitions should be a JSON object with all value nodes.");
              ret.add("-D" + entry.getKey() + "=" + entry.getValue().asText());
            });
  }

  /** Converts the given JsonNode into a set of --argument strings, within the given prefix. */
  private static void convertArgument(
      @Nonnull String prefix, @Nonnull JsonNode node, @Nonnull List<? super String> ret) {
    if (node.isObject()) {
      node.fields()
          .forEachRemaining(
              (entry) -> {
                String name = entry.getKey();
                JsonNode childnode = entry.getValue();
                convertArgument(prefix + "-" + name, childnode, ret);
              });
    } else if (node.isArray()) {
      ArrayNode arrayNode = (ArrayNode) node;
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (int j = 0; j < arrayNode.size(); j++) {
        JsonNode childNode = arrayNode.get(j);
        if (!childNode.isValueNode())
          throw new IllegalArgumentException(
              "Array in json response file can only contain strings");
        if (first) {
          sb.append(childNode.textValue());
          first = false;
        } else sb.append(":").append(childNode.textValue());
      }
      ret.add(prefix);
      ret.add(sb.toString());
    } else {
      ret.add(prefix);
      if (!node.textValue().isEmpty()) ret.add(node.textValue());
    }
  }

  public static void save(ConnectorArguments arguments) throws IOException {
    File responseFile = FileUtils.getFile(arguments.getResponseFileName());
    newObjectMapper().writeValue(responseFile, from_arguments(arguments.getArgs()));
    LOG.info("Saved response file to {}", responseFile.getAbsolutePath());
  }

  public static ResponseFileEntity from_arguments(String[] args) {
    ResponseFileEntity root = ResponseFileEntity.OBJECT.get();
    String prev = null;
    for (String arg : args) {
      if (arg.startsWith("-")) {
        if (prev != null) root.add(prev);
        prev = arg;
      } else {
        if (prev != null) {
          root.add(prev, arg);
          prev = null;
        }
      }
    }
    if (prev != null) root.add(prev);

    return root;
  }

  public static class ResponseFileEntity {

    public static final Supplier<ResponseFileEntity> OBJECT =
        () -> new ResponseFileEntity(new LinkedHashMap<>(), null);
    public static final Function<String, ResponseFileEntity> VALUE =
        value -> new ResponseFileEntity(null, value);

    private final Map<String, ResponseFileEntity> fields;
    private final String value;

    private ResponseFileEntity(Map<String, ResponseFileEntity> fields, String value) {
      this.fields = fields;
      this.value = value;
    }

    @JsonValue
    public Object getValue() {
      if (fields != null) return fields;
      return value;
    }

    public void put(String[] tokens, String value) {
      if (tokens.length == 1) {
        put(tokens[0], value);
      } else {
        fields
            .computeIfAbsent(tokens[0], j -> OBJECT.get())
            .put(ArrayUtils.subarray(tokens, 1, tokens.length), value);
      }
    }

    private void put(String token, String value) {
      fields.put(token, new ResponseFileEntity(null, value));
    }

    public void add(String flag) {
      add(flag, Boolean.TRUE.toString());
    }

    public void add(String argument, String value) {

      if (argument.equals("--save-response-file")) return; // don't save over and over again

      if (argument.startsWith("-D")) {
        argument = argument.substring(2);
        int idx = argument.indexOf('=');
        ResponseFileEntity definitions = fields.computeIfAbsent("definitions", d -> OBJECT.get());
        definitions.put(argument.substring(0, idx), argument.substring(idx + 1));
      } else {
        argument = argument.substring(2);
        String[] tokens = StringUtils.split(argument, "-");
        put(tokens, value);
      }
    }
  }
}
