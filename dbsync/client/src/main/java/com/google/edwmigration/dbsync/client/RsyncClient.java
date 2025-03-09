package com.google.edwmigration.dbsync.client;

import com.google.edwmigration.dbsync.server.CloudRunServerAPI;
import com.google.edwmigration.dbsync.common.InstructionGenerator;
import com.google.edwmigration.dbsync.common.storage.LocalStorage;
import com.google.edwmigration.dbsync.proto.Checksum;
import com.google.edwmigration.dbsync.server.GCSTarget;
import com.google.edwmigration.dbsync.server.GcsServerMain.Mode;
import com.google.edwmigration.dbsync.server.RsyncTarget;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.edwmigration.dbsync.storage.gcs.GcsStorage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.NotImplementedException;

public class RsyncClient {

  private static final int MIN_SIZE_TO_RSYNC = 10000;
  private static final int CHECKSUM_BLOCK_SIZE = 4096;

  // DTS calls this.
  // GcsClientMain calls this.
  // Validation calls this.
  // TODO support multiple files ?
  public void putRsync(
      String projectId,
      String location,
      URI sourceUri,
      URI stagingBucket,
      URI targetUri,
      boolean delete_staging_files)
      throws IOException, ExecutionException, InterruptedException {
    ByteSource byteSource;
    switch (sourceUri.getScheme()) {
      case "file":
        LocalStorage storage = new LocalStorage();
        byteSource = storage.newByteSource(sourceUri);
        break;
      case "gcs":
      case "s3":
        throw new NotImplementedException("URI Scheme not supported yet");
      default:
        throw new IllegalStateException("Unexpected URI Scheme: " + sourceUri.getScheme());
    }

    CloudRunServerAPI server =
        new CloudRunServerAPI(projectId, location, stagingBucket, targetUri, delete_staging_files);
    GcsStorage gcsStorage = new GcsStorage(projectId);
    Path sourceFilePath = Paths.get(sourceUri);
    if (!gcsStorage.checkFileExists(targetUri.toString())
        || byteSource.size() < MIN_SIZE_TO_RSYNC) {
      // if file doesn't exist we can't run rsync since there is nothing in target
      // if file is small its more efficient to just copy than run rsync
      gcsStorage.uploadFile(sourceFilePath, targetUri);
      return;
    }
    // TODO move this out of the put rsync and into a initialization function to be called once
    server.deployRsyncJobs();
    putRsync(byteSource, server, new GCSTarget(projectId, targetUri, stagingBucket));
  }

  private void putRsync(ByteSource source, CloudRunServerAPI server, RsyncTarget target)
      throws IOException, ExecutionException, InterruptedException {

    // generate target checksum
    server.generate();

    // read checksum from gcs
    List<Checksum> targetChecksums = new ArrayList<>();
    ByteSource checksumSource = target.getChecksumByteSource();
    try (InputStream checksumStream = checksumSource.openBufferedStream()) {
      Checksum checksum;
      while ((checksum = Checksum.parseDelimitedFrom(checksumStream)) != null) {
        targetChecksums.add(checksum);
      }
    }

    // Invoke InstructionGenerator
    InstructionGenerator generator = new InstructionGenerator(CHECKSUM_BLOCK_SIZE);
    ByteSink instructionSink = target.getInstructionsByteSink();
    try (OutputStream instructionStream = instructionSink.openBufferedStream()) {
      generator.generate(
          instruction -> {
            try {
              instruction.writeDelimitedTo(instructionStream);
            } catch (IOException e) {
              throw new RuntimeException("Failed to write instructions", e);
            }
          },
          source,
          targetChecksums);
    }

    // Reconstruct on GCS
    server.reconstruct();

    server.deleteJob(Mode.GENERATE);
    server.deleteJob(Mode.RECEIVE);
  }
}
