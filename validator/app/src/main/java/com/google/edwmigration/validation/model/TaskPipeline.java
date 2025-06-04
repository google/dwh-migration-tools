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

import java.util.ArrayList;
import java.util.List;

/**
 * A composable, functional-style pipeline for executing a sequence of named tasks.
 *
 * <p>This is effectively a type-safe "task runner" that accumulates and executes a chain of
 * `NamedTask<I, O>` transformations. The pipeline ensures clean propagation of errors via the
 * {@link Either} monad: once a failure occurs, all subsequent steps are skipped and the pipeline
 * short-circuits with a Left value.
 *
 * <p>This approach allows you to declaratively describe your business logic as a chain of
 * meaningful steps, each wrapped with a name, and avoid nested try-catch or procedural flow
 * control.
 *
 * <p>Example:
 *
 * <pre>{@code
 * TaskPipeline<UserInputContext, ExecutionState> pipeline =
 *     TaskPipeline.start()
 *         .then(validateConnection)
 *         .then(initExecutionState)
 *         .thenAll(dataPreparationTasks)
 *         .then(runValidation);
 *
 * Either<Failure, ExecutionState> result = pipeline.run(input);
 * }</pre>
 *
 * @param <I> Input type for the pipeline (first task)
 * @param <O> Output type for the pipeline (final result)
 */
public class TaskPipeline<I, O> {

  /** Internal list of tasks for debugging/logging visibility. */
  private final List<NamedTask<?, ?>> tasks = new ArrayList<>();

  /** The composed runner function that accumulates task logic. */
  private final FunctionWithError<I, O> runner;

  private TaskPipeline(FunctionWithError<I, O> runner, List<NamedTask<?, ?>> existing) {
    this.runner = runner;
    this.tasks.addAll(existing);
  }

  /**
   * Initializes a new task pipeline with identity input and a successful {@link Either}.
   *
   * @param <T> The type of the input and output
   * @return A pipeline ready to accept tasks
   */
  public static <T> TaskPipeline<T, T> start() {
    return new TaskPipeline<>(Either::right, List.of());
  }

  /**
   * Appends a new task to the pipeline.
   *
   * @param task A named task transforming O -> N
   * @param <N> The new output type after applying this task
   * @return A new pipeline with the task appended
   */
  public <N> TaskPipeline<I, N> then(NamedTask<O, N> task) {
    List<NamedTask<?, ?>> newTasks = new ArrayList<>(this.tasks);
    newTasks.add(task);

    // Compose the runner so that the new task runs only if the previous steps succeeded
    FunctionWithError<I, N> newRunner = input -> this.runner.apply(input).flatMap(task::run);
    return new TaskPipeline<>(newRunner, newTasks);
  }

  /**
   * Appends multiple tasks from a list to the pipeline. Useful when tasks are defined in a
   * collection or generated programmatically.
   *
   * @param taskList List of tasks to run sequentially
   * @return A new pipeline with all tasks chained
   */
  public TaskPipeline<I, O> thenAllFrom(List<NamedTask<O, O>> taskList) {
    TaskPipeline<I, O> pipeline = this;
    for (NamedTask<O, O> task : taskList) {
      pipeline = pipeline.then(task);
    }
    return pipeline;
  }

  /**
   * Executes the pipeline against a given input.
   *
   * @param input The starting input for the pipeline
   * @return Either a successful result (Right) or a failure (Left)
   */
  public Either<Failure, O> run(I input) {
    return runner.apply(input);
  }

  /**
   * Functional interface representing a computation that returns either a result or an error.
   *
   * @param <I> Input type
   * @param <O> Output type
   */
  @FunctionalInterface
  public interface FunctionWithError<I, O> {
    Either<Failure, O> apply(I input);
  }

  /**
   * Exposes the accumulated task list.
   *
   * @return List of all named tasks in the pipeline, in order
   */
  public List<NamedTask<?, ?>> getTasks() {
    return tasks;
  }
}
