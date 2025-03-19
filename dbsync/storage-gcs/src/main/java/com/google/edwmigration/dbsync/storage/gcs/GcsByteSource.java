package com.google.edwmigration.dbsync.storage.gcs;

import com.google.cloud.ReadChannel;
import com.google.common.base.Optional;
import com.google.edwmigration.dbsync.common.storage.AbstractRemoteByteSource;
import com.google.edwmigration.dbsync.common.storage.Slice;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.io.ByteSource;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsByteSource extends AbstractRemoteByteSource {

  private static final boolean DEBUG = false;

  private static final Logger LOG = LoggerFactory.getLogger(GcsByteSource.class);

  private final Storage storage;
  private final BlobId blobId;
  private InputStream sourceStream;

  private long currentSourceOffset = 0;

  private GcsByteSource(Storage storage, BlobId blobId, @Nullable Slice slice) {
    super(slice);
    this.storage = storage;
    this.blobId = blobId;
  }

  private GcsByteSource(Storage storage, BlobId blobId, @Nullable Slice slice,
      InputStream sourceStream, long currentSourceOffset) {
    super(slice);
    this.storage = storage;
    this.blobId = blobId;
    this.sourceStream = sourceStream;
    this.currentSourceOffset = currentSourceOffset;
  }

  public GcsByteSource(Storage storage, BlobId blobId) {
    this(storage, blobId, null);
  }

  @Override
  protected ByteSource slice(Slice slice) {
    return new GcsByteSource(storage, blobId, slice, sourceStream, currentSourceOffset);
  }

  @Override
  public InputStream openStream() throws IOException {
    ReadChannel channel = storage.reader(blobId);
    Slice slice = getSlice();
    if (slice != null) {
      channel.seek(slice.getOffset());
      channel = channel.limit(slice.getLength());
    }
    sourceStream = Channels.newInputStream(channel);
    currentSourceOffset = slice.getOffset();
    return sourceStream;
  }

  @Override
  public long copyTo(OutputStream outputStream) throws IOException {
    Slice slice = getSlice();
    if (slice == null) {
      return super.copyTo(outputStream);
    }

    if (slice.getOffset() < currentSourceOffset) {
      if (DEBUG) {
        LOG.info(String.format("Target offset %d smaller than current offset %d, reopen stream",
            slice.getOffset(), currentSourceOffset));
      }
      sourceStream.close();
      sourceStream = Channels.newInputStream(storage.reader(blobId));
      currentSourceOffset = 0;
    }

    // Skip forward to the desired start.
    long toSkip = slice.getOffset() - currentSourceOffset;
    skip(toSkip);
    return copy(slice.getLength(), outputStream);
  }

  @Override
  protected MoreObjects.ToStringHelper toStringHelper(ToStringHelper helper) {
    return super.toStringHelper(helper)
        .add("blobId", blobId);
  }

  @Override
  public Optional<Long> sizeIfKnown() {
    return Optional.of(storage.get(blobId).asBlobInfo().getSize());
  }

  private void skip(long length) throws IOException {
    long remaining = length;
    while (remaining > 0) {
      long skipped = sourceStream.skip(remaining);
      if (skipped < remaining) {
        throw new EOFException("Unexpected end of stream while skipping.");
      }
      remaining -= skipped;
      currentSourceOffset += skipped;
    }
  }

  private long copy(long copyLength, OutputStream out) throws IOException {
    // Now copy exactly copyLength bytes.
    byte[] buffer = new byte[8192];
    long remaining = copyLength;
    while (remaining > 0) {
      int bytesToRead = (int) Math.min(buffer.length, remaining);
      int read = sourceStream.read(buffer, 0, bytesToRead);
      if (read == -1) {
        throw new EOFException("Unexpected end of stream while copying " + copyLength
            + " bytes starting at offset " + getSlice().getOffset());
      }
      out.write(buffer, 0, read);
      remaining -= read;
      currentSourceOffset += read;
    }

    // As we always read until there's no remaining bytes or throw.
    return copyLength;
  }
}
