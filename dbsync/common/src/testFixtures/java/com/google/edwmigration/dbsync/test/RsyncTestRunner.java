package com.google.edwmigration.dbsync.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.edwmigration.dbsync.common.ChecksumGenerator;
import com.google.edwmigration.dbsync.common.InstructionGenerator;
import com.google.edwmigration.dbsync.common.InstructionReceiver;
import com.google.edwmigration.dbsync.proto.Checksum;
import com.google.edwmigration.dbsync.proto.Instruction;
import com.google.common.base.Joiner;
import com.google.common.io.ByteSource;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RsyncTestRunner {
  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(RsyncTestRunner.class);

  public static ByteSource newRandomData(int size) {
    byte[] data = new byte[size];
    ThreadLocalRandom.current().nextBytes(data);
    return ByteSource.wrap(data);
  }

  public enum Flag {
    PrintServerRaw,
    PrintClientRaw,
    PrintServerNewRaw,
    PrintChecksums,
    PrintInstructions,
  }

  private final String name;
  private final ByteSource serverData;
  private final ByteSource clientData;
  private int blockSize = 4096;

  private final EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);

  public RsyncTestRunner(String name, ByteSource serverData, ByteSource clientData) {
    this.name = name;
    this.serverData = serverData;
    this.clientData = clientData;
  }

  public void setBlockSize(int blockSize) {
    this.blockSize = blockSize;
  }

  public void setFlags(Flag... flags) {
    Collections.addAll(this.flags, flags);
  }

  public boolean isFlag(Flag flag) {
    return flags.contains(flag);
  }

  private List<Checksum> checksum() throws Exception{
    if (isFlag(Flag.PrintServerRaw))
      logger.debug("Server (initial) data is " + Arrays.toString(serverData.read()));
    ChecksumGenerator generator = new ChecksumGenerator(blockSize);
    List<Checksum> checksums = new ArrayList<>();
    generator.generate(checksums::add, serverData);
    if (isFlag(Flag.PrintChecksums))
      logger.debug("Server checksums are " + checksums);
    return checksums;
  }

  private List<Instruction> instruct(List<Checksum> checksums) throws  Exception {
    if (isFlag(Flag.PrintClientRaw))
      logger.debug("Client data is " + Arrays.toString(clientData.read()));
    List<Instruction> instructions = new ArrayList<>();
    InstructionGenerator matcher = new InstructionGenerator(blockSize);
    matcher.generate(instructions::add, clientData, checksums);
    if (isFlag(Flag.PrintInstructions))
      logger.debug("Client instructions are\n" + Joiner.on('\n').join(instructions));
    int instructionSize = 0;
    for (Instruction instruction : instructions)
      instructionSize += instruction.getSerializedSize();
    logger.debug("Client instructions size is " + instructionSize);
    return instructions;
  }

  private void reconstruct(OutputStream out, List<Instruction> instructions) throws Exception {
    try (InstructionReceiver receiver = new InstructionReceiver(out, serverData)) {
      for (Instruction instruction : instructions) {
        // logger.info("Instruction: " + instruction.toPrettyString());
        receiver.receive(instruction);
      }
    }
  }

  private byte[] reconstruct(List<Instruction> instructions) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    reconstruct(out, instructions);
    byte[] serverDataNew = out.toByteArray();
    if (isFlag(Flag.PrintServerNewRaw))
      logger.debug("Server (result) data is " + Arrays.toString(serverDataNew));
    logger.debug("Server (result) data size is " + serverDataNew.length);
    assertArrayEquals(clientData.read(), serverDataNew, name + " got mismatched data.");
    return serverDataNew;
  }

  public byte[] run() throws Exception {
    logger.debug("Starting rsync " + name);
    List<Checksum> checksums = checksum();
    List<Instruction> instructions = instruct(checksums);
    return reconstruct(instructions);
  }

  public void run(OutputStream serverDataNew) throws Exception {
    List<Checksum> checksums = checksum();
    List<Instruction> instructions = instruct(checksums);
    reconstruct(serverDataNew, instructions);
  }

}
