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

/**
 * A functional interface representing a computation or operation that transforms an input of type
 * {@code I} into an output of type {@code O}, potentially throwing an exception.
 *
 * <p>This abstraction is intended for sequential composition of operations in a pipeline-like
 * fashion. It is similar in spirit to {@code Function<I, O>}, but allows for checked exceptions
 * during execution.
 *
 * @param <I> The input type to the task
 * @param <O> The output type of the task
 */
@FunctionalInterface
public interface Task<I, O> {

  /**
   * Runs the task with the given input and produces an output.
   *
   * @param input The input to process
   * @return The output of the task
   * @throws Exception If the task encounters an error during execution
   */
  O run(I input) throws Exception;

  /**
   * Composes this task with another task. The output of this task becomes the input to the next.
   * This allows multiple tasks to be chained together sequentially.
   *
   * <p>For example, if task A outputs a {@code String} and task B takes a {@code String} and
   * returns an {@code Integer}, then {@code A.then(B)} produces a new task from A’s input type to
   * B’s output type.
   *
   * @param next The next task to execute after this one
   * @param <N> The output type of the composed task
   * @return A new composed task
   */
  default <N> Task<I, N> then(Task<O, N> next) {
    return input -> next.run(this.run(input));
  }

  /**
   * Returns a no-op identity task that simply returns the input unmodified. Useful as a neutral
   * element for task chaining.
   *
   * @param <T> The input/output type
   * @return A task that returns its input unchanged
   */
  static <T> Task<T, T> identity() {
    return input -> input;
  }
}
