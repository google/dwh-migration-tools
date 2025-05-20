package com.google.edwmigration.dbsync.common.storage;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.io.ByteSource;
import com.google.errorprone.annotations.ForOverride;
import javax.annotation.CheckReturnValue;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class AbstractRemoteByteSource extends ByteSource {

  private final Slice slice;

  public AbstractRemoteByteSource(Slice slice) {
    this.slice = slice;
  }

  public @Nullable Slice getSlice() {
    return slice;
  }

  @Override
  public Optional<Long> sizeIfKnown() {
    if (slice != null) return Optional.of(slice.getLength());
    return super.sizeIfKnown();
  }

  protected abstract ByteSource slice(Slice slice);

  @Override
  @CheckReturnValue
  public ByteSource slice(long offset, long length) {
    Slice slice = Slice.reslice(this.slice, offset, length);
    return slice(slice);
  }

  @ForOverride
  protected MoreObjects.ToStringHelper toStringHelper(MoreObjects.ToStringHelper helper) {
    return helper;
  }

  @Override
  public String toString() {
    MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
    helper = toStringHelper(helper);
    if (slice != null) helper.add("slice", slice);
    return helper.toString();
  }
}
