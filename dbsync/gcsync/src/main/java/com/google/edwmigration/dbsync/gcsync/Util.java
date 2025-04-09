package com.google.edwmigration.dbsync.gcsync;

import com.google.common.io.ByteSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class Util {

  public static List<String> getListOfFiles(ByteSource byteSource) throws IOException {
    List<String> files = new ArrayList<>();

    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(byteSource.openStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        files.add(line);
      }
    }
    return files;
  }

  public static Path getTemporaryCheckSumFilePath(Path file) {
    return file.resolveSibling(getCheckSumFileName(file.getFileName().toString()));
  }

  public static String getCheckSumFileName(String fileName) {
    return String.format("%s.%s", fileName, Constants.CHECK_SUM_FILE_SUFFIX);
  }

  public static String getInstructionFileName(String fileName) {
    return String.format("%s.%s", fileName, Constants.INSTRUCTION_FILE_SUFFIX);
  }

  public static String getTempFileName(String fileName) {
    return String.format("%s.%s", fileName, Constants.TMP_FILE_SUFFIX);
  }

  public static String ensureTrailingSlash(String uri) {
    return uri.endsWith("/") ? uri : uri + "/";
  }
}
