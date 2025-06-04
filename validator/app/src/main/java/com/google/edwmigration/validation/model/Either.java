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

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A functional data structure representing a value of one of two possible types: Left (commonly
 * used for error or failure) or Right (commonly used for success or result).
 *
 * <p>This class is often used as a lightweight alternative to exceptions for flow control, enabling
 * error accumulation or short-circuiting in chains of transformations.
 *
 * <p>Inspired by functional programming patterns in languages like Haskell and Scala.
 *
 * @param <L> The type of the "Left" (usually error or failure)
 * @param <R> The type of the "Right" (usually success or result)
 */
public abstract class Either<L, R> {

  /** @return true if this is a Left (i.e. contains a failure or error) */
  public abstract boolean isLeft();

  /** @return true if this is a Right (i.e. contains a success value) */
  public abstract boolean isRight();

  /**
   * @return the Left value
   * @throws IllegalStateException if this is a Right
   */
  public abstract L getLeft();

  /**
   * @return the Right value
   * @throws IllegalStateException if this is a Left
   */
  public abstract R getRight();

  /** Factory method to construct a Left value */
  public static <L, R> Either<L, R> left(L value) {
    return new Left<>(value);
  }

  /** Factory method to construct a Right value */
  public static <L, R> Either<L, R> right(R value) {
    return new Right<>(value);
  }

  /**
   * Applies a transformation to the Right value if present.
   *
   * @param mapper A function from R to T
   * @param <T> The type of the result if mapped
   * @return A new Either (Left is unchanged; Right is transformed)
   */
  public <T> Either<L, T> map(Function<R, T> mapper) {
    if (isRight()) {
      return right(mapper.apply(getRight()));
    } else {
      return left(getLeft());
    }
  }

  /**
   * Applies a transformation to the Right value if present, where the transformation itself
   * produces another Either (i.e. chaining).
   *
   * @param mapper A function from R to Either<L, T>
   * @param <T> The type of the result if mapped
   * @return A new Either (Left is unchanged; Right is replaced with result of mapper)
   */
  public <T> Either<L, T> flatMap(Function<R, Either<L, T>> mapper) {
    if (isRight()) {
      return mapper.apply(getRight());
    } else {
      return left(getLeft());
    }
  }

  /**
   * Folds the Either into a single value by applying the appropriate function based on its variant.
   *
   * @param onLeft function to apply to a Left value
   * @param onRight function to apply to a Right value
   * @param <T> The resulting type after folding
   * @return Result of applying either onLeft or onRight
   */
  public <T> T fold(Function<? super L, T> onLeft, Function<? super R, T> onRight) {
    return isRight() ? onRight.apply(getRight()) : onLeft.apply(getLeft());
  }

  /**
   * Performs a side-effect if this is a Right value (commonly used for logging or debugging).
   *
   * @param onRight action to perform on the Right value
   * @return this (for fluent chaining)
   */
  public Either<L, R> peek(Consumer<? super R> onRight) {
    if (isRight()) onRight.accept(getRight());
    return this;
  }

  /** Nicely formatted debug output */
  @Override
  public String toString() {
    return isRight() ? "Right(" + getRight() + ")" : "Left(" + getLeft() + ")";
  }

  /** Private implementation of a Left (holds the error case) */
  private static class Left<L, R> extends Either<L, R> {
    private final L value;

    private Left(L value) {
      this.value = value;
    }

    public boolean isLeft() {
      return true;
    }

    public boolean isRight() {
      return false;
    }

    public L getLeft() {
      return value;
    }

    public R getRight() {
      throw new IllegalStateException("Cannot getRight() from Left");
    }
  }

  /** Private implementation of a Right (holds the success case) */
  private static class Right<L, R> extends Either<L, R> {
    private final R value;

    private Right(R value) {
      this.value = value;
    }

    public boolean isLeft() {
      return false;
    }

    public boolean isRight() {
      return true;
    }

    public L getLeft() {
      throw new IllegalStateException("Cannot getLeft() from Right");
    }

    public R getRight() {
      return value;
    }
  }
}
