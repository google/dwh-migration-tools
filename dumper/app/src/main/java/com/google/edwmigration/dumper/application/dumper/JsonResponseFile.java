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
package com.google.edwmigration.dumper.application.dumper;

import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import joptsimple.OptionDescriptor;
import joptsimple.OptionSpec;
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

  private static final Logger logger = LoggerFactory.getLogger(JsonResponseFile.class);

  private static final String OPTION_TERMINATOR = "--";

  /** Options whose names contain one of these words hold secrets. */
  private static final Pattern SECRET_OPTION_NAME = Pattern.compile("password|secret|token");

  /**
   * Finds credentials in a URL: user:password@, Oracle's user/password@, or a word that names a
   * credential parameter.
   */
  private static final Pattern URL_WITH_CREDENTIALS =
      Pattern.compile("[^:/@]+[:/][^:/@]+@|pass|pwd|secret|token|key", Pattern.CASE_INSENSITIVE);

  private static final ImmutableSet<PosixFilePermission> OWNER_READ_WRITE =
      Sets.immutableEnumSet(OWNER_READ, OWNER_WRITE);

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
  private static ImmutableList<String> to_arguments(@Nonnull File file) throws IOException {
    return convert(newObjectMapper().readTree(file));
  }

  @VisibleForTesting
  /* pp */ static ImmutableList<String> to_arguments(@Nonnull String text)
      throws IOException, JsonProcessingException {
    return convert(newObjectMapper().readTree(text));
  }

  @Nonnull
  private static ImmutableList<String> convert(@Nonnull JsonNode node) {
    List<String> out = new ArrayList<>();
    convert(node, out);
    return ImmutableList.copyOf(out);
  }

  private static void convert(@Nonnull JsonNode node, @Nonnull List<String> ret) {
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
      String s, String s1, String s2, JsonNode childnode, List<String> ret) {
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
      @Nonnull String prefix, @Nonnull JsonNode node, @Nonnull List<String> ret) {
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
          sb.append(childNode.asText());
          first = false;
        } else sb.append(":").append(childNode.asText());
      }
      ret.add(prefix);
      ret.add(sb.toString());
    } else {
      ret.add(prefix);
      // A null value, such as "assessment:" in YAML, gives the option alone.
      String value = node.isNull() ? "" : node.asText();
      if (!value.isEmpty()) ret.add(value);
    }
  }

  /**
   * Saves the command-line arguments to the response file, without secret values. The file is
   * readable and writable by its owner only where the file system supports POSIX permissions.
   */
  public static void save(ConnectorArguments arguments) throws IOException {
    Path responseFile = FileUtils.getFile(arguments.getResponseFileName()).toPath();
    String[] args = removeSecrets(arguments.getArgs(), arguments.parser.recognizedOptions());
    restrictToOwner(responseFile);
    try (OutputStream out = Files.newOutputStream(responseFile)) {
      newObjectMapper().writeValue(out, from_arguments(args));
    }
    logger.info("Saved response file to {}", responseFile.toAbsolutePath());
  }

  /**
   * Removes the values of secret options, and a --url that carries credentials, from the
   * command-line arguments. A secret option with an optional value, such as --password, is kept
   * without its value, so the dumper prompts for the value when the response file is used.
   */
  @Nonnull
  private static String[] removeSecrets(
      @Nonnull String[] args, @Nonnull Map<String, OptionSpec<?>> options) {
    ImmutableSet<String> secretNames = findSecretOptionNames(options);
    List<String> kept = new ArrayList<>();
    ImmutableSet.Builder<String> removed = ImmutableSet.builder();
    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if (arg.equals(OPTION_TERMINATOR)) {
        // The remaining arguments are not options, and the dumper does not use them.
        break;
      }

      OptionDescriptor option = findOption(options, arg);
      if (option == null) {
        kept.add(arg);
        continue;
      }

      String value = findInlineValue(arg);
      boolean valueIsNext =
          value == null
              && i + 1 < args.length
              && (option.requiresArgument()
                  || (option.acceptsArguments() && !looksLikeOption(args[i + 1])));
      if (valueIsNext) {
        i++;
        value = args[i];
      }

      String name = option.options().get(0);
      boolean secret =
          !Collections.disjoint(option.options(), secretNames)
              || (name.equals(ConnectorArguments.OPT_URI)
                  && value != null
                  && URL_WITH_CREDENTIALS.matcher(value).find());
      if (secret) {
        removed.add("--" + name);
        if (!option.requiresArgument()) {
          // An empty value is saved as the option alone.
          kept.add("--" + name);
          kept.add("");
        }
      } else {
        kept.add(arg);
        if (valueIsNext) {
          kept.add(value);
        }
      }
    }

    ImmutableSet<String> removedNames = removed.build();
    if (!removedNames.isEmpty()) {
      logger.warn(
          "The response file does not contain the values of {}. Supply them again when using it.",
          removedNames);
    }
    return kept.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
  }

  /** Returns the names of the options whose values are secrets. */
  @VisibleForTesting
  @Nonnull
  static ImmutableSet<String> findSecretOptionNames(@Nonnull Map<String, OptionSpec<?>> options) {
    ImmutableSet.Builder<String> names = ImmutableSet.builder();
    for (String name : options.keySet()) {
      if (SECRET_OPTION_NAME.matcher(name).find()) {
        names.add(name);
      }
    }
    return names.build();
  }

  /**
   * Finds the option that the argument names, the way joptsimple does: by its full name or a unique
   * abbreviation, after one or two hyphens and before an optional '='.
   */
  @CheckForNull
  private static OptionDescriptor findOption(
      @Nonnull Map<String, OptionSpec<?>> options, @Nonnull String arg) {
    if (!arg.startsWith("-")) {
      return null;
    }

    String name = StringUtils.substringBefore(arg.substring(arg.startsWith("--") ? 2 : 1), "=");
    OptionSpec<?> option = options.get(name);
    if (option == null) {
      for (Map.Entry<String, OptionSpec<?>> entry : options.entrySet()) {
        if (entry.getKey().startsWith(name)) {
          if (option != null && option != entry.getValue()) {
            return null;
          }
          option = entry.getValue();
        }
      }
    }
    return (OptionDescriptor) option;
  }

  /** Tells whether joptsimple reads the argument as an option rather than as a value. */
  private static boolean looksLikeOption(@Nonnull String arg) {
    return arg.startsWith("-") && !arg.equals("-");
  }

  @CheckForNull
  private static String findInlineValue(@Nonnull String arg) {
    int index = arg.indexOf('=');
    return index < 0 ? null : arg.substring(index + 1);
  }

  /**
   * Makes the file readable and writable by its owner only, creating it if needed, where the file
   * system supports POSIX permissions.
   */
  private static void restrictToOwner(@Nonnull Path file) throws IOException {
    if (!file.getFileSystem().supportedFileAttributeViews().contains("posix")) {
      return;
    }

    if (!Files.exists(file)) {
      Files.createFile(file, PosixFilePermissions.asFileAttribute(OWNER_READ_WRITE));
      return;
    }

    try {
      Files.setPosixFilePermissions(file, OWNER_READ_WRITE);
    } catch (IOException e) {
      // Some mounts, such as FAT, reject permission changes; the file holds no secrets.
      logger.warn("Unable to make {} readable by its owner only: {}", file, e.getMessage());
    }
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
        // The definition follows -D in the same argument or comes as the next one.
        String definition = argument.length() > 2 ? argument.substring(2) : value;
        int idx = definition.indexOf('=');
        if (idx < 0) {
          // ConnectorProperties ignores a definition without '=', so there is nothing to save.
          return;
        }
        ResponseFileEntity definitions = fields.computeIfAbsent("definitions", d -> OBJECT.get());
        definitions.put(definition.substring(0, idx), definition.substring(idx + 1));
      } else {
        argument = argument.substring(2);
        String[] tokens = StringUtils.split(argument, "-");
        put(tokens, value);
      }
    }
  }
}
