package com.google.edwmigration.dbsync.storage.gcs;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Optional;
import com.google.common.io.ByteSource;
import com.google.edwmigration.dbsync.common.storage.AbstractRemoteByteSource;
import com.google.edwmigration.dbsync.common.storage.Slice;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import org.checkerframework.checker.nullness.qual.Nullable;

public class GcsByteSource extends AbstractRemoteByteSource {

  private final Storage storage;
  private final BlobId blobId;

  private GcsByteSource(Storage storage, BlobId blobId, @Nullable Slice slice) {
    super(slice);
    this.storage = storage;
    this.blobId = blobId;
  }

  public GcsByteSource(Storage storage, BlobId blobId) {
    this(storage, blobId, null);
  }

  @Override
  protected ByteSource slice(Slice slice) {
    return new GcsByteSource(storage, blobId, slice);
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
  protected MoreObjects.ToStringHelper toStringHelper(ToStringHelper helper) {
    return super.toStringHelper(helper).add("blobId", blobId);
  }

  @Override
  public Optional<Long> sizeIfKnown() {
    return Optional.of(storage.get(blobId).asBlobInfo().getSize());
  }
}
