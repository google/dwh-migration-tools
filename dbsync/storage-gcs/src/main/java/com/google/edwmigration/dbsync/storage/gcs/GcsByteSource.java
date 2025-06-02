package com.google.edwmigration.dbsync.storage.gcs;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Optional;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.edwmigration.dbsync.common.storage.AbstractRemoteByteSource;
import com.google.edwmigration.dbsync.common.storage.Slice;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import org.checkerframework.checker.nullness.qual.Nullable;

public class GcsByteSource extends AbstractRemoteByteSource {

  private static final int CHUNK_SIZE = 16 * 1024 * 1024; // 16 Mib

  private final Storage storage;
  private final BlobId blobId;
  private InputStreamCache inputStreamCache;

  private GcsByteSource(Storage storage, BlobId blobId, @Nullable Slice slice) {
    super(slice);
    this.storage = storage;
    this.blobId = blobId;
    ReadChannel readChannel = storage.reader(blobId);
    readChannel.setChunkSize(CHUNK_SIZE);
    this.inputStreamCache =
        new InputStreamCache(Channels.newInputStream(readChannel), 0, CHUNK_SIZE);
  }

  private GcsByteSource(
      Storage storage, BlobId blobId, @Nullable Slice slice, InputStreamCache inputStreamCache) {
    super(slice);
    this.storage = storage;
    this.blobId = blobId;
    this.inputStreamCache = inputStreamCache;
  }

  public GcsByteSource(Storage storage, BlobId blobId) {
    this(storage, blobId, null);
  }

  @Override
  protected ByteSource slice(Slice slice) {
    return new GcsByteSource(storage, blobId, slice, inputStreamCache);
  }

  @Override
  public InputStream openStream() throws IOException {
    ReadChannel channel = storage.reader(blobId);
    Slice slice = getSlice();
    if (slice != null) {
      channel.seek(slice.getOffset());
      channel = channel.limit(slice.getLength() + slice.getOffset());
    }
    return Channels.newInputStream(channel);
  }

  @Override
  public long copyTo(OutputStream outputStream) throws IOException {
    Slice slice = getSlice();
    if (slice == null) {
      return super.copyTo(outputStream);
    }

    // If the target offset is before the current pointer or if it's after the last byte that has
    // been cached, we re-position the stream which would result in a new HTTP ranged read.
    if (slice.getOffset() < inputStreamCache.getCurrentOffset()
        || slice.getOffset() > inputStreamCache.getLastByteOffset()) {
      inputStreamCache.getSourceStream().close();
      inputStreamCache.setSourceStream(repositionStream(slice.getOffset()));
      inputStreamCache.setCurrentOffset(slice.getOffset());
      inputStreamCache.setLastByteOffset(slice.getOffset() + CHUNK_SIZE);
    }

    // Skip forward to the desired start.
    long toSkip = slice.getOffset() - inputStreamCache.getCurrentOffset();
    skip(toSkip);
    return copy(slice.getLength(), outputStream);
  }

  @Override
  public long copyTo(ByteSink byteSink) throws IOException {
    try (OutputStream outputStream = byteSink.openBufferedStream()) {
      return copyTo(outputStream);
    }
  }

  @Override
  protected MoreObjects.ToStringHelper toStringHelper(ToStringHelper helper) {
    return super.toStringHelper(helper).add("blobId", blobId);
  }

  @Override
  public Optional<Long> sizeIfKnown() {
    return Optional.of(storage.get(blobId).asBlobInfo().getSize());
  }

  private InputStream repositionStream(long offset) throws IOException {
    ReadChannel channel = storage.reader(blobId);
    Slice slice = getSlice();
    if (slice != null) {
      channel.seek(offset);
    }
    return Channels.newInputStream(channel);
  }

  private void skip(long length) throws IOException {
    long remaining = length;
    while (remaining > 0) {
      long skipped = inputStreamCache.getSourceStream().skip(remaining);
      if (skipped == 0) {
        throw new EOFException("Unexpected end of stream while skipping.");
      }
      remaining -= skipped;
      inputStreamCache.setCurrentOffset(inputStreamCache.getCurrentOffset() + skipped);
    }
  }

  private long copy(long copyLength, OutputStream out) throws IOException {
    // Now copy exactly copyLength bytes.
    byte[] buffer = new byte[8192];
    long remaining = copyLength;
    while (remaining > 0) {
      int bytesToRead = (int) Math.min(buffer.length, remaining);
      int read = inputStreamCache.getSourceStream().read(buffer, 0, bytesToRead);
      if (read == -1) {
        throw new EOFException(
            "Unexpected end of stream while copying "
                + copyLength
                + " bytes starting at offset "
                + getSlice().getOffset());
      }
      out.write(buffer, 0, read);
      remaining -= read;
      inputStreamCache.setCurrentOffset(inputStreamCache.getCurrentOffset() + read);
    }

    // As we always read until there's no remaining bytes or throw.
    return copyLength;
  }

  /**
   * A cache with an {@link InputStream} and a mark of the current offset within the stream. This
   * cache will be shared by this {@link ByteSource} and the copies created by {@link #slice(long,
   * long)} to efficiently implement the {@link #copyTo(OutputStream)} method.
   */
  private static class InputStreamCache {

    private InputStream sourceStream;
    private long currentOffset;

    // The last byte that has been cached or would have been cached as part of the ranged read.
    private long lastByteOffset;

    private InputStreamCache(InputStream inputStream, long currentOffSet, long lastByteOffset) {
      this.sourceStream = inputStream;
      this.currentOffset = currentOffSet;
      this.lastByteOffset = lastByteOffset;
    }

    private InputStream getSourceStream() {
      return sourceStream;
    }

    public long getCurrentOffset() {
      return currentOffset;
    }

    public void setSourceStream(InputStream inputStream) {
      this.sourceStream = inputStream;
    }

    public void setCurrentOffset(long currentOffset) {
      this.currentOffset = currentOffset;
    }

    public void setLastByteOffset(long lastByteOffset) {
      this.lastByteOffset = lastByteOffset;
    }

    public long getLastByteOffset() {
      return lastByteOffset;
    }
  }
}
