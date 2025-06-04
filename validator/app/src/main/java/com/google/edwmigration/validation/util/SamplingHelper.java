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
package com.google.edwmigration.validation.util;

/**
 * Utility class for statistical sampling, particularly for discrepancy detection using binomial
 * probability models.
 */
public final class SamplingHelper {

  private SamplingHelper() {
    // Utility class, do not instantiate
  }

  /**
   * Calculates the minimum number of samples required to detect a discrepancy rate with a given
   * confidence level using a binomial approximation.
   *
   * <p>This method assumes sampling with replacement (binomial model). It returns the number of
   * samples (X) needed to ensure that if discrepancies exist at a rate of {@code
   * minDiscrepancyRate}, the probability of missing all of them is no greater than {@code (1 -
   * confidence)}.
   *
   * @param confidence The desired confidence level (e.g., 0.99 for 99% confidence).
   * @param minDiscrepancyRate The minimum discrepancy rate to detect (e.g., 0.0001 for 0.01%).
   * @return The number of rows to sample to achieve the desired detection power.
   * @throws IllegalArgumentException if confidence or discrepancy rate is out of (0, 1) bounds.
   */
  public static long calculateDetectionSampleSize(double confidence, double minDiscrepancyRate) {
    if (confidence <= 0.0 || confidence >= 1.0) {
      throw new IllegalArgumentException("Confidence must be between 0 and 1 (exclusive)");
    }
    if (minDiscrepancyRate <= 0.0 || minDiscrepancyRate >= 1.0) {
      throw new IllegalArgumentException("Discrepancy rate must be between 0 and 1 (exclusive)");
    }

    double beta = 1.0 - confidence;
    double sampleSize = Math.log(beta) / Math.log(1.0 - minDiscrepancyRate);
    return (long) Math.ceil(sampleSize);
  }
}
