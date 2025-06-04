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
package com.google.edwmigration.validation;

import static org.junit.Assert.*;

import com.google.edwmigration.validation.util.SamplingHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SampleSizeCalculatorTest {

  @Test
  public void returns_approximately_69_000_rows_for_999_confidence_for_one_in_10_000() {
    double confidence = 0.999;
    double minDiscrepancyRate = 0.0001;
    long expectedMin = 68000;
    long expectedMax = 70000;

    long sampleSize = SamplingHelper.calculateDetectionSampleSize(confidence, minDiscrepancyRate);

    assertTrue(
        "Sample size for 99.9% confidence and PD=0.0001 should be in expected range. Found: "
            + sampleSize,
        sampleSize >= expectedMin && sampleSize <= expectedMax);
  }

  @Test
  public void returns_approximately_3_000_rows_for_95_confidence_for_one_in_1_000() {
    double confidence = 0.95;
    double minDiscrepancyRate = 0.001;
    long expectedMin = 2900;
    long expectedMax = 3100;

    long sampleSize = SamplingHelper.calculateDetectionSampleSize(confidence, minDiscrepancyRate);

    assertTrue(
        "Sample size for 95% confidence and PD=0.001 should be in expected range. Found: "
            + sampleSize,
        sampleSize >= expectedMin && sampleSize <= expectedMax);
  }

  @Test
  public void returns_approximately_240_rows_for_90_confidence_for_one_percent_discrepancy() {
    double confidence = 0.90;
    double minDiscrepancyRate = 0.01;
    long expectedMin = 225;
    long expectedMax = 250;

    long sampleSize = SamplingHelper.calculateDetectionSampleSize(confidence, minDiscrepancyRate);

    assertTrue(
        "Sample size for 90% confidence and PD=0.01 should be in expected range. Found: "
            + sampleSize,
        sampleSize >= expectedMin && sampleSize <= expectedMax);
  }

  @Test
  public void returns_large_sample_size_when_detecting_very_rare_discrepancy() {
    double confidence = 0.99;
    double minDiscrepancyRate = 0.00001;

    long sampleSize = SamplingHelper.calculateDetectionSampleSize(confidence, minDiscrepancyRate);

    assertTrue(
        "Sample size for very small discrepancy rate should be very large. Found: " + sampleSize,
        sampleSize > 400000);
  }

  @Test(expected = IllegalArgumentException.class)
  public void throws_error_when_confidence_is_zero() {
    double invalidConfidence = 0.0;
    double minDiscrepancyRate = 0.001;
    SamplingHelper.calculateDetectionSampleSize(invalidConfidence, minDiscrepancyRate);
  }

  @Test(expected = IllegalArgumentException.class)
  public void throws_error_when_confidence_is_one() {
    double invalidConfidence = 1.0;
    double minDiscrepancyRate = 0.001;
    SamplingHelper.calculateDetectionSampleSize(invalidConfidence, minDiscrepancyRate);
  }

  @Test(expected = IllegalArgumentException.class)
  public void throws_error_when_discrepancy_rate_is_negative() {
    double confidence = 0.99;
    double invalidRate = -0.0001;
    SamplingHelper.calculateDetectionSampleSize(confidence, invalidRate);
  }

  @Test(expected = IllegalArgumentException.class)
  public void throws_error_when_discrepancy_rate_is_one_or_greater() {
    double confidence = 0.99;
    double invalidRate = 1.0;
    SamplingHelper.calculateDetectionSampleSize(confidence, invalidRate);
  }
}
