/*
 * Copyright 2022-2024 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.connector.hdfs;

/**
 * Simple synchronization counter used instead of {@link java.util.concurrent.Phaser} as Phazer has
 * this limitation to count up to 2^16, while this counter counts up to 2^32.
 */
class AtomicCounter {

  private int currentCount;

  AtomicCounter(int currentCount) {
    this.currentCount = currentCount;
  }

  synchronized void increment() {
    assert currentCount < Integer.MAX_VALUE : "Implementation limits reached!";
    ++currentCount;
  }

  synchronized void decrement() {
    if (--currentCount <= 0) {
      notify();
    }
  }

  /** Non-busily blocks current thread until currentCounter becomes <= 0 */
  synchronized void waitTillZero() {
    while (currentCount > 0) {
      try {
        wait(1000);
      } catch (InterruptedException exn) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Waiting for counter to become 0 failed.", exn);
      }
    }
  }
}
