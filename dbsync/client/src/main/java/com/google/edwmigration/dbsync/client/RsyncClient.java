package com.google.edwmigration.dbsync.client;

import static com.google.edwmigration.dbsync.client.CloudRunHelper.getChecksumURI;
import static com.google.edwmigration.dbsync.client.CloudRunHelper.getInstructionURI;

import com.google.edwmigration.dbsync.client.CloudRunHelper.Mode;
import com.google.edwmigration.dbsync.common.InstructionGenerator;
import com.google.edwmigration.dbsync.common.storage.LocalStorage;
import com.google.edwmigration.dbsync.proto.Checksum;
import com.google.edwmigration.dbsync.proto.Instruction;
import com.google.edwmigration.dbsync.storage.gcs.GcsStorage;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.NotImplementedException;

public class RsyncClient {

  private static final int MIN_SIZE_TO_RSYNC = 1;
  private static final int CHECKSUM_BLOCK_SIZE = 4096;


  // DTS calls this.
  // RsyncClientMain calls this.
  // Validation calls this.
  public void putRsync(String projectId, URI sourceUri, URI stagingBucket, URI targetURI)
      throws IOException, URISyntaxException {
    ByteSource byteSource;
    switch (sourceUri.getScheme()) {
      case "file" -> {
        LocalStorage storage = new LocalStorage();
        byteSource = storage.newByteSource(sourceUri);
      }
      case "gcs", "s3" -> throw new NotImplementedException("URI Scheme not supported yet");
      default -> throw new IllegalStateException("Unexpected URI Scheme: " + sourceUri.getScheme());
    }

    if (byteSource.size() < MIN_SIZE_TO_RSYNC) {
      throw new IllegalStateException("dont handle small files");
    } else {
      putRsync(projectId, byteSource, stagingBucket, targetURI);
    }
  }

  private void putRsync(String projectId, ByteSource source, URI stagingBucket, URI targetUri)
      throws IOException, URISyntaxException {
    // deploy all cloudrun jobs
    try {
      CloudRunHelper.deployRsyncJobs(projectId, stagingBucket, targetUri);
    } catch (Exception e) {
      Logger.getLogger("rsync").log(Level.SEVERE, e.getMessage(), e);
    }

    // generate remote checksum remotely using cloud run
    try {
      CloudRunHelper.runRsyncJob(projectId, "us-west1", Mode.GENERATE);
    } catch (Exception e) {
      Logger.getLogger("rsync").log(Level.SEVERE, e.getMessage(), e);
    }

    // read checksum from gcs
    GcsStorage stagingStorage = new GcsStorage(projectId);
    List<Checksum> targetChecksums = new ArrayList<>();
    ByteSource checksumSource = stagingStorage.newByteSource(
        getChecksumURI(stagingBucket, targetUri));
    try (InputStream checksumStream = checksumSource.openBufferedStream()) {
      Checksum checksum;
      while ((checksum = Checksum.parseDelimitedFrom(checksumStream)) != null) {
        targetChecksums.add(checksum);
      }
    }

    // Invoke InstructionGenerator
    InstructionGenerator generator = new InstructionGenerator(CHECKSUM_BLOCK_SIZE);
    List<Instruction> instructions = new ArrayList<>();
    generator.generate(instructions::add, source, targetChecksums);
    ByteSink instructionSink = stagingStorage.newByteSink(
        getInstructionURI(stagingBucket, targetUri));
    try (OutputStream instructionStream = instructionSink.openBufferedStream()) {
      for (Instruction instruction : instructions) {
        instruction.writeDelimitedTo(instructionStream);
      }
    }

    // Reconstruct on GCS using cloudrun
    try {
      CloudRunHelper.runRsyncJob(projectId, "us-west1", Mode.RECEIVE);
    } catch (Exception e) {
      Logger.getLogger("rsync").log(Level.SEVERE, e.getMessage(), e);
    }
  }
}
