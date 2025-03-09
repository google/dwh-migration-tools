package com.google.edwmigration.dbsync.server;

import com.google.edwmigration.dbsync.common.ChecksumGenerator;
import com.google.edwmigration.dbsync.common.InstructionReceiver;
import com.google.edwmigration.dbsync.proto.Instruction;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;

public class RsyncServer {
  private static final int CHECKSUM_BLOCK_SIZE = 8192;
  private final RsyncTarget target;
  private final boolean deleteStagingFiles;

  public RsyncServer(RsyncTarget target, boolean deleteStagingFiles) {
    this.target = target;
    this.deleteStagingFiles = deleteStagingFiles;
  }

  public void generate() throws IOException {
    ByteSource source = target.getTargetByteSource();
    ByteSink checksumSink = target.getChecksumByteSink();
    try (OutputStream checksumStream = checksumSink.openBufferedStream()) {
      ChecksumGenerator generator = new ChecksumGenerator(CHECKSUM_BLOCK_SIZE);
      generator.generate(
          checksum -> {
            try {
              // TODO: Switch to simple write to save storage and read by byte size
              checksum.writeDelimitedTo(checksumStream);
            } catch (IOException e) {
              throw new RuntimeException("Failed to generate checksum", e);
            }
          },
          source);
    }
  }

  public void reconstruct() throws IOException {
    ByteSource instructionSource = target.getInstructionsByteSource();
    ByteSource targetByteSource = target.getTargetByteSource();
    ByteSink stagingByteSink = target.getStagingByteSink();

    try (OutputStream stagingFileStream = stagingByteSink.openBufferedStream();
        InputStream instructionFileStream = instructionSource.openBufferedStream()) {
      InstructionReceiver receiver = new InstructionReceiver(stagingFileStream, targetByteSource);
      Instruction instruction;
      while ((instruction = Instruction.parseDelimitedFrom(instructionFileStream)) != null) {
        receiver.receive(instruction);
      }
    }
    // Once instructions are applied and staging file is created with latest data
    // Target file needs to be replaces with the staged file
    // And other temp files in staging folder would be deleted
    target.moveStagedFileToTarget(this.deleteStagingFiles);
  }
}
