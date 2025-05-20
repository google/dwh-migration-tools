package com.google.edwmigration.dbsync.common.storage;

import com.google.common.base.MoreObjects;
import com.google.common.math.LongMath;
import javax.annotation.CheckReturnValue;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;

public class Slice {

  @CheckReturnValue
  public static Slice reslice(@Nullable Slice slice, long offset, long length) {
    if (slice != null) {
      // We are re-slicing a slice. This probably never happens in rsync.
      offset = LongMath.checkedAdd(slice.getOffset(), offset);
      long end = LongMath.checkedAdd(offset, length);
      end = Math.min(slice.getEndExclusive(), end);
      length = LongMath.checkedSubtract(end, offset);
    }
    return new Slice(offset, length);
  }

  private final long offset;
  private final long length;

  public Slice(long offset, long length) {
    this.offset = offset;
    this.length = length;
  }

  public @NonNegative long getOffset() {
    return offset;
  }

  public @NonNegative long getLength() {
    return length;
  }

  public @NonNegative long getEndExclusive() {
    return LongMath.checkedAdd(getOffset(), getLength());
  }

  public @NonNegative long getEndInclusive() {
    return LongMath.checkedSubtract(getEndExclusive(), 1);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("offset", getOffset())
        .add("length", getLength())
        .toString();
  }
}
