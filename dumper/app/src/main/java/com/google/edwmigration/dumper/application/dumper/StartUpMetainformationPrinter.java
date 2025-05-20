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

import static java.util.jar.Attributes.Name.IMPLEMENTATION_TITLE;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Enumeration;
import java.util.Objects;
import java.util.jar.Manifest;
import org.springframework.core.io.ClassPathResource;

public class StartUpMetainformationPrinter {

  public static void printMetainfo() {
    try {
      printBanner();
      printMetainfFile();
    } catch (Exception ignore) {
    }
  }

  private static void printBanner() {
    try {
      ClassPathResource classPathResource =
          new ClassPathResource("/banner/banner.txt", StartUpMetainformationPrinter.class);
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(classPathResource.getInputStream()))) {
        reader.lines().forEach(System.out::println);
      }
    } catch (Exception ignore) {
    }
  }

  private static void printMetainfFile() {
    Manifest manifest = loadCurrentClassManifest();
    if (manifest == null) {
      return;
    }

    String buildDate = manifest.getMainAttributes().getValue("Build-Date-UTC");
    String change = manifest.getMainAttributes().getValue("Change");
    String version = manifest.getMainAttributes().getValue("Implementation-Version");

    System.out.println("App version: [" + version + "], change: [" + change + "]");
    System.out.println("Build date: " + buildDate);
    System.out.println();
  }

  private static Manifest loadCurrentClassManifest() {
    try {
      final String implementationTitle =
          StartUpMetainformationPrinter.class.getPackage().getImplementationTitle();
      final ClassLoader classLoader = StartUpMetainformationPrinter.class.getClassLoader();

      Enumeration<URL> manifestResources = classLoader.getResources("META-INF/MANIFEST.MF");
      while (manifestResources.hasMoreElements()) {
        try (InputStream inputStream = manifestResources.nextElement().openStream()) {
          Manifest manifest = new Manifest(inputStream);
          String currTitle = manifest.getMainAttributes().getValue(IMPLEMENTATION_TITLE);
          if (Objects.equals(implementationTitle, currTitle)) {
            return manifest;
          }
        } catch (Exception ignore) {
        }
      }
    } catch (Exception ignore) {
    }

    return null;
  }
}
