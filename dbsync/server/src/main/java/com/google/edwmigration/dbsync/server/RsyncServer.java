package com.google.edwmigration.dbsync.server;

import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.edwmigration.dbsync.common.ChecksumGenerator;
import com.google.edwmigration.dbsync.common.InstructionReceiver;
import com.google.edwmigration.dbsync.proto.Instruction;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class RsyncServer {
  private static final int CHECKSUM_BLOCK_SIZE = 4096;
  private RsyncTarget target;

  public RsyncServer(RsyncTarget target) {
    this.target = target;
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
    ByteSource baseData = target.getTargetByteSource();
    ByteSink stagingsink = target.getStagingByteSink();

    try (OutputStream targetStream = stagingsink.openBufferedStream();
        InputStream instructionStream = instructionSource.openBufferedStream()) {
      InstructionReceiver receiver = new InstructionReceiver(targetStream, baseData);
      Instruction instruction;
      while ((instruction = Instruction.parseDelimitedFrom(instructionStream)) != null) {
        receiver.receive(instruction);
      }
    }
  }
}
