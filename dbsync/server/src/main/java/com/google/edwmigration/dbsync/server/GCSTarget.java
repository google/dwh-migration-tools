package com.google.edwmigration.dbsync.server;

import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.edwmigration.dbsync.storage.gcs.GcsStorage;
import java.net.URI;
import org.checkerframework.checker.units.qual.C;

public class GCSTarget implements RsyncTarget {

  private final GcsStorage storage;
  private final URI targetUri;
  private final URI checksumUri;
  private final URI instructionUri;
  private final URI stagedFileURI;

  private static final String INSTRUCTION_FILE_NAME = "instruction";
  private static final String CHECKSUM_FILE_NAME = "checksum";
  private static final String STAGED_FILE_NAME = "staged";

  public GCSTarget(String project, URI targetUri, URI stagingBucket) {
    this.storage = new GcsStorage(project);
    this.targetUri = targetUri;
    this.instructionUri = stagingBucket.resolve(targetUri.getPath()).resolve(INSTRUCTION_FILE_NAME);
    this.checksumUri = stagingBucket.resolve(targetUri.getPath()).resolve(CHECKSUM_FILE_NAME);
    this.stagedFileURI = stagingBucket.resolve(targetUri.getPath()).resolve(STAGED_FILE_NAME);
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
    return storage.newByteSink(stagedFileURI);
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
}
