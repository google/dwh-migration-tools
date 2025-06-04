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
package com.google.edwmigration.validation.model;

import java.util.function.Function;

/**
 * Represents a named unit of work that transforms an input of type {@code I} into an output of type
 * {@code O}, wrapped in an {@link Either} to handle success or failure.
 *
 * <p>This class enables composable, named tasks that can be chained together into a pipeline. Each
 * task handles its own error propagation using the {@code Either<Failure, O>} type instead of
 * throwing exceptions.
 *
 * @param <I> The input type of the task
 * @param <O> The output type of the task
 */
public class NamedTask<I, O> {

  /** A human-readable name for the task, used for logging and diagnostics. */
  public final String name;

  /** The core logic function for this task, from input {@code I} to {@code Either<Failure, O>}. */
  private final Function<I, Either<Failure, O>> logic;

  /** Private constructor; use {@link #of} to create tasks. */
  private NamedTask(String name, Function<I, Either<Failure, O>> logic) {
    this.name = name;
    this.logic = logic;
  }

  /**
   * Factory method to create a named task.
   *
   * @param name A human-readable task name
   * @param logic The transformation logic, returning an {@code Either<Failure, O>} to model
   *     success/failure
   * @param <I> Input type
   * @param <O> Output type
   * @return A new {@code NamedTask<I, O>}
   */
  public static <I, O> NamedTask<I, O> of(String name, Function<I, Either<Failure, O>> logic) {
    return new NamedTask<>(name, logic);
  }

  /**
   * Executes the task logic with the given input.
   *
   * @param input The input value of type {@code I}
   * @return The result of the task, either a success ({@code Right<O>}) or an error message ({@code
   *     Left<Failure>})
   */
  public Either<Failure, O> run(I input) {
    return logic.apply(input);
  }

  /**
   * Chains this task to another task, forming a composite task that runs both in sequence. If this
   * task fails, the second task is not run.
   *
   * @param nextName The name of the composed task
   * @param nextLogic The logic to apply to this task's output if successful
   * @param <N> The output type of the next task
   * @return A new {@code NamedTask<I, N>} that chains this task to the next
   */
  public <N> NamedTask<I, N> then(String nextName, Function<O, Either<Failure, N>> nextLogic) {
    return NamedTask.of(
        nextName,
        (I input) -> {
          Either<Failure, O> result = this.run(input);
          return result.flatMap(nextLogic);
        });
  }
}
