package com.google.edwmigration.dbsync.gcsync;

import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
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

  public static void writeMd5Header(OutputStream outputStream, String md5) throws IOException {
    outputStream.write((md5 + "\n").getBytes());
  }

  public static boolean verifyMd5Header(ByteSource byteSource, String md5) throws IOException {
    if (byteSource.isEmpty()) {
      return false;
    }
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(byteSource.openStream()))) {
      reader.read();
      String md5Stored = reader.readLine();
      return md5Stored.equals(md5);
    }
  }

  public static String skipMd5Header(InputStream inputStream) throws IOException {
    String md5 = "";
    try (InputStreamReader reader = new InputStreamReader(inputStream)) {
      while (true) {
        int c = reader.read();
        if (c == -1) {
          throw new IOException("Unexpected EOF");
        }
        if (c == '\n') {
          break;
        }
        md5 += (char) c;
      }
    }
    return md5;
  }

  public static String ensureTrailingSlash(String uri) {
    return uri.endsWith("/") ? uri : uri + "/";
  }
}
