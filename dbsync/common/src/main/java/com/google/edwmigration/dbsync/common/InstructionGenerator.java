package com.google.edwmigration.dbsync.common;

import com.google.edwmigration.dbsync.proto.BlockLocation;
import com.google.edwmigration.dbsync.proto.Checksum;
import com.google.edwmigration.dbsync.proto.Instruction;
import com.google.common.hash.HashCode;
import com.google.common.io.ByteSource;
import com.google.protobuf.ByteString;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import org.checkerframework.checker.index.qual.NonNegative;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InstructionGenerator {

  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(InstructionGenerator.class);
  private static final boolean DEBUG = false;

  private final int blockSize;

  public InstructionGenerator(int blockSize) {
    this.blockSize = blockSize;
  }

  private static Instruction newLiteralInstruction(byte[] literalBuffer, int literalBufferLength) {
    return Instruction.newBuilder()
        .setData(ByteString.copyFrom(literalBuffer, 0, literalBufferLength))
        .build();
  }

  public void generate(Consumer<? super Instruction> out, ByteSource in,
      List<? extends Checksum> checksums) throws IOException {
    Int2ObjectMap<Collection<Checksum>> checksumMap = new Int2ObjectOpenHashMap<>(checksums.size());
    for (Checksum c : checksums) {
      checksumMap.computeIfAbsent(c.getWeakChecksum(), k -> new ArrayList<>()).add(c);
    }
    if (DEBUG) {
      LOG.debug("Checksum map contains {} keys", checksumMap.size());
    }

    // TODO: We could use Literal.MAX_LENGTH here, but that would make testing a bit more fiddly.
    int blockSize = checksums.isEmpty() ? this.blockSize : checksums.get(0).getBlockLength();

    @NonNegative int rollingBlockLength;
    final byte[] literalBuffer = new byte[Math.min(blockSize,
        AlgorithmConstants.MAX_LITERAL_LENGTH)];
    @NonNegative int literalBufferLength = 0;

    // int lastBlockSize = checksums.get(checksums.size() - 1).getBlockLength();
    RollingChecksumImpl rollingChecksum = new RollingChecksumImpl(blockSize);
    try (InputStream i = in.openBufferedStream()) {
      STREAM:
      for (; ; ) {
        // This is the fast-path.
        rollingBlockLength = rollingChecksum.reset(i);
        if (DEBUG) {
          LOG.debug("Fast-path reset read {} bytes", rollingBlockLength);
        }
        BYTE:
        for (; ; ) {
          Collection<? extends Checksum> cc = checksumMap.get(rollingChecksum.getWeakHashCode());
          // Grant me the serenity to accept the Optional<T>s I cannot change,
          // The courage to change the ones I can avoid without getting fired,
          // And the wisdom to know the difference.
          MATCH:
          if (cc != null) {
            HashCode strongHashCode = rollingChecksum.getStrongHashCode();
            for (Checksum c : cc) {
              if (c.getStrongChecksum().equals(strongHashCode.toString())) {
                if (literalBufferLength > 0) {
                  if (DEBUG) {
                    LOG.debug("Emitting pre-match literal");
                  }
                  out.accept(newLiteralInstruction(literalBuffer, literalBufferLength));
                  literalBufferLength = 0;
                }
                Instruction insn = Instruction.newBuilder()
                    .setBlockLocation(BlockLocation.newBuilder()
                            .setBlockOffset(c.getBlockOffset())
                            .setBlockLength(c.getBlockLength()))
                    .build();
                out.accept(insn);
                continue STREAM;
              }
            }
          }
          int b = i.read();
          if (b < 0) {
            break STREAM;
          }
          if (literalBufferLength == literalBuffer.length) {
            // We're about to run out of RollingChecksum buffer.
            if (DEBUG) {
              LOG.debug("Emitting overflow literal");
            }
            out.accept(newLiteralInstruction(literalBuffer, literalBufferLength));
            literalBufferLength = 0;
          }
          literalBuffer[literalBufferLength++] = rollingChecksum.roll((byte) b);
        }
      }

      // We know that lastSentOffset is less than one block from the end,
      // because if it were more than one block, we would have emitted an overflow literal above.
      if (literalBufferLength > 0) {
        if (DEBUG) {
          LOG.debug("Emitting trailing literal");
        }
        out.accept(newLiteralInstruction(literalBuffer, literalBufferLength));
      }
      if (rollingBlockLength > 0) {
        if (DEBUG) {
          LOG.debug("Emitting trailing rolling buffer");
        }
        // TODO: Double copy with ByteString.copyFrom().
        byte[] rollingBuffer = rollingChecksum.getBlock(rollingBlockLength);
        out.accept(newLiteralInstruction(rollingBuffer, rollingBuffer.length));
      }
    }
  }
}
