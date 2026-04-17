<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# DWH Migration tools — Agent Instructions

## Architecture

### Modules overview

- **README** (`README.md`): Information about repository tools.
- **CVEs** (`.security/`): Known fals positive CVES in the repository.
- **GitHub workflows** (`.github/workflows`): GitHub Actions configuration.
- **Dumper** (`dumper/app/`): Main DWH Dumper executable module. The module contains multiple connectors for different data source as `Oracle`, `Hive`, `Snowflake` and so on.
- **Dumper** (`dumper/lib-ext-hive-metastore/`): Dedicated module for Hive-Metastore with generated code.
- **Connectors** (`dumper/app/java/com/google/edwmigration/dumper/application/dumper/connector`): Connectors root location.

### High-Sensitivity Areas

- **`ConnectorArguments`**: ??
- **`Connector`** ??
TBD

## Design Patterns

- **`CloseableIterable`** over `Stream`: Always close iterables.
- **Null over `Optional`**: Use `null` for missing values. `Optional` is not used.
- **Builder pattern**: For complex creation. Never require passing `null` for optional parameters.
- **Package-private by default**: Only make things public with demonstrated need.
- **Postel's Law**: Accept case-insensitive input, produce canonical output.
- **Validate at boundaries**: `Preconditions` at public entry points; internal methods assume invariants hold.
- TBD

## Configuration

- **Always** use `Java 8` for compilation and code style checks. 

## Coding Conventions

### Naming

- Method names describe specific behavior: `selectInIdOrder` not `selectOrdered`.
- Avoid `get` prefix — use `find`, `fetch`, `load`, `parse`, `create`, or drop it.
- Variable names indicate meaning, not type. Property names use kebab-case.
- `toString()` must produce parseable output.
- Avoid `Factory` suffix unless the class is a true factory.

### Code Style

- 2 spaces indent, 4 spaces continuation. Empty newline after control flow blocks.
- Use `this.` for instance field assignment. `Preconditions` calls first in methods.
- No `final` on locals. No one-argument-per-line unless necessary.
- Magic numbers should be named constants. No personal pronouns in comments.
- `} else {` on same line. Minimize variable scope. `try-with-resources` for all `AutoCloseable`.
- Prefer method references over lambdas. Wrap lines at the highest semantic level.
- Always use imports — never use fully-qualified class names inline.
- Static imports are preferable for static method usage except `Preconditions`.
- `for`, `while` loops are preferable over java streams.
- Immutable collections from Google Guava like `ImmutableList`, `ImmutableSet` and so forth are preferable over `java.lang.collections.*`.

### Serialization

TBD (Streaming?)

### Error Handling

- Messages: direct, actionable, with specific values. Capitalize first word.
- Don't swallow exceptions. Use `closeQuietly` for cleanup that shouldn't mask real failures.
- Close iterables in `finally`. Separate `Preconditions` calls for each condition.
TBD

### Testing

- Test classes and methods should be package private unless required by inheritance.
- Compute expected values, don't hardcode. Tests belong in the module that owns the code.
- Write the most direct test for the bug. Parameterized tests for type variations.
- Use JUnit 4 for unit-tests.
- `WireMockServer` is preferable to use for http request testing.


## Commands

- **Build (no tests):** `./gradlew build -x test `
- **Build Dumper (no tests):** `./gradlew :dumper:app:build -x test `
- **Format code:** `./gradlew spotlessApply`
- **Format Dumper code:** `./gradlew :dumper:app:spotlessApply`
- **Check formatting:** `./gradlew spotlessCheck`
- **Check Dumper formatting:** `./gradlew :dumper:app:spotlessCheck`

## PR & Commit Conventions

- PR titles follow `[b/number]: Description` format (e.g., `[b/7238234] Improve Snowflake report...`, `[b/7238234] Refactore Connectors models...`).
- Commit messages describe the *what* and *why*, not implementation details.
- Copyright and Apache License header required on all new files (enforced by spotless pre-commit hook).

## Boundaries

- **Never** modify `LICENSE`, `NOTICE` without explicit discussion.
- **Never** commit secrets, credentials, or cloud-specific tokens.
- **Ask first** before adding new third-party dependencies (license compatibility matters).
- **Ask first** before promoting package-private classes/methods to public.