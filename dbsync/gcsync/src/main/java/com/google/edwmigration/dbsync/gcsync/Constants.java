package com.google.edwmigration.dbsync.gcsync;

import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;

public class Constants {

  public static final String FILES_TO_RSYNC_FILE_NAME = "filesToRsync.txt";

  public static final String JAR_FILE_NAME = "gcsync-all.jar";

  public static final String CHECK_SUM_FILE_SUFFIX = "checksum";

  public static final String INSTRUCTION_FILE_SUFFIX = "instruction";

  public static final String TMP_FILE_SUFFIX = "updated";

  public static final int BLOCK_SIZE = 4 * 4096;

  // 10 MiB
  public static final long RSYNC_SIZE_THRESHOLD = 10 * 1024 * 1024;

  public static final String GENERATE_CHECK_SUM_MAIN = GenerateCheckSumMain.class.getName();

  public static final String RECONSTRUCT_FILE_MAIN = ReconstructFilesMain.class.getName();

  public static final String CLOUD_RUN_TIMEOUT = "3600s";

  public static final Duration EXTRA_POLLING_TIMEOUT = Durations.fromMinutes(5);
}
