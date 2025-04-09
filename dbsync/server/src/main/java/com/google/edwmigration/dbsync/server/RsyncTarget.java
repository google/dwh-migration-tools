package com.google.edwmigration.dbsync.server;

import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;

public interface RsyncTarget {
  ByteSource getChecksumByteSource();

  ByteSink getChecksumByteSink();

  ByteSource getInstructionsByteSource();

  ByteSink getInstructionsByteSink();

  ByteSource getTargetByteSource();

  ByteSink getStagingByteSink();
}
