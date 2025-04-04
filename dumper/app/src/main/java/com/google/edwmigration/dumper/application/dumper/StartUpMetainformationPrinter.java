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

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.jar.Manifest;

public class StartUpMetainformationPrinter {
  private static final String BANNER_PATH = "/banner/banner.txt";

  public static void printMetainfo() {
    URLClassLoader cl = (URLClassLoader) StartUpMetainformationPrinter.class.getClassLoader();
    try {
      List<String> bannerLines = readBannerLines();
      bannerLines.forEach(System.out::println);

      URL url = cl.findResource("META-INF/MANIFEST.MF");
      Manifest manifest = new Manifest(url.openStream());
      String buildDate = manifest.getMainAttributes().getValue("Build-Date-UTC");
      String change = manifest.getMainAttributes().getValue("Change");
      String version = manifest.getMainAttributes().getValue("Implementation-Version");

      System.out.println("App version: [" + version + "], change: [" + change + "]");
      System.out.println("Build date: " + buildDate);
    } catch (Exception ignore) {
    }
  }

  private static List<String> readBannerLines() throws IOException, URISyntaxException {
    URL resource = StartUpMetainformationPrinter.class.getResource(BANNER_PATH);
    return Files.readAllLines(Paths.get(resource.toURI()));
  }
}
