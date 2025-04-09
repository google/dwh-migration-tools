package com.google.edwmigration.dbsync.storage.aws;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.io.ByteSource;
import com.google.edwmigration.dbsync.common.storage.AbstractRemoteByteSource;
import com.google.edwmigration.dbsync.common.storage.Slice;
import java.io.IOException;
import java.io.InputStream;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

public class S3ByteSource extends AbstractRemoteByteSource {
  private final S3Client s3;
  private final String bucket;
  private final String key;

  private S3ByteSource(S3Client s3, String bucket, String key, @Nullable Slice slice) {
    super(slice);
    this.s3 = s3;
    this.bucket = bucket;
    this.key = key;
  }

  public S3ByteSource(S3Client s3, String bucket, String key) {
    this(s3, bucket, key, null);
  }

  @Override
  protected ByteSource slice(Slice slice) {
    return new S3ByteSource(s3, bucket, key, slice);
  }

  private static @Nullable String toRange(@Nullable Slice slice) {
    if (slice == null) return null;
    return "bytes=" + slice.getOffset() + "-" + slice.getEndInclusive();
  }

  @Override
  public InputStream openStream() throws IOException {
    try {
      GetObjectRequest objectRequest =
          GetObjectRequest.builder().bucket(bucket).key(key).range(toRange(getSlice())).build();
      return s3.getObject(objectRequest);
    } catch (NoSuchBucketException | NoSuchKeyException e) {
      throw S3Utils.newFileNotFoundException("No such S3 object s3://" + bucket + "/" + key, e);
    } catch (SdkException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected MoreObjects.ToStringHelper toStringHelper(ToStringHelper helper) {
    return super.toStringHelper(helper).add("bucket", bucket).add("key", key);
  }
}
