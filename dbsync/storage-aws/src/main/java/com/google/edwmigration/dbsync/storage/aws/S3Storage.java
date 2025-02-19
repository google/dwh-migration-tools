package com.google.edwmigration.dbsync.storage.aws;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import java.net.URI;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

// https://www.baeldung.com/java-google-cloud-storage
public class S3Storage {

  public static final String SCHEME = "s3";

  private final S3Client s3;

  public S3Storage(Region region) {
    this.s3 = S3Client.builder().region(region).build();
  }

  public @NonNull ByteSource newByteSource(URI uri) {
    Preconditions.checkArgument(SCHEME.equals(uri.getScheme()));
    String bucket = uri.getAuthority();
    String path = uri.getPath();
    return new S3ByteSource(s3, bucket, path);
  }

  public @NonNull ByteSink newByteSink(URI uri) {
    Preconditions.checkArgument(SCHEME.equals(uri.getScheme()));
    String bucket = uri.getAuthority();
    String path = uri.getPath();
    return new S3ByteSink(s3, bucket, path);
  }

}
