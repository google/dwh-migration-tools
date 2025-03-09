package com.google.edwmigration.dbsync.server;

import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.edwmigration.dbsync.common.UriUtil;
import com.google.edwmigration.dbsync.storage.gcs.GcsStorage;
import java.net.URI;

public class GCSTarget implements RsyncTarget {

  private final GcsStorage storage;
  private final URI targetUri;
  private final URI checksumUri;
  private final URI instructionUri;
  private final URI stagedFileUri;
  private static final String INSTRUCTION_FILE_NAME = "instruction";
  private static final String CHECKSUM_FILE_NAME = "checksum";
  private static final String STAGED_FILE_NAME = "staged";

  public GCSTarget(String project, URI targetUri, URI stagingBucket) {
    this.storage = new GcsStorage(project);
    this.targetUri = targetUri;

    // Create a staging folder path for each target file using the target path and file name
    // Then the checksum, instruction and staged files are created in this folder
    // Example of staging files storage:
    //  If target files path is gs://target_bucket/dir/target/file.txt
    //  Staging bucket is gs://target_bucket/
    //  Then staging files will be stored as gs://target_bucket/dir/target/file.txt/staged
    String targetRelativePath = UriUtil.getRelativePath(targetUri);
    String stagingBasePath = UriUtil.ensureTrailingSlash(targetRelativePath);
    this.instructionUri = stagingBucket.resolve(stagingBasePath + INSTRUCTION_FILE_NAME);
    this.checksumUri = stagingBucket.resolve(stagingBasePath + CHECKSUM_FILE_NAME);
    this.stagedFileUri = stagingBucket.resolve(stagingBasePath + STAGED_FILE_NAME);
  }

  @Override
  public ByteSink getChecksumByteSink() {
    return storage.newByteSink(checksumUri);
  }

  @Override
  public ByteSource getChecksumByteSource() {
    return storage.newByteSource(checksumUri);
  }

  @Override
  public ByteSink getStagingByteSink() {
    return storage.newByteSink(stagedFileUri);
  }

  @Override
  public ByteSource getTargetByteSource() {
    return storage.newByteSource(targetUri);
  }

  @Override
  public ByteSource getInstructionsByteSource() {
    return storage.newByteSource(instructionUri);
  }

  @Override
  public ByteSink getInstructionsByteSink() {
    return storage.newByteSink(instructionUri);
  }

  @Override
  public void moveStagedFileToTarget(boolean deleteStagingFiles) {
    storage.copyFile(stagedFileUri, targetUri);
    if (deleteStagingFiles) {
      storage.deleteFile(stagedFileUri);
      storage.deleteFile(checksumUri);
      storage.deleteFile(instructionUri);
    }
  }
}
