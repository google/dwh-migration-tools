package com.google.edwmigration.dbsync.storage.aws;

import com.google.common.io.ByteSink;
import java.io.IOException;
import software.amazon.awssdk.services.s3.S3Client;

public class S3ByteSink extends ByteSink {
  private final S3Client s3;
  private final String bucket;
  private final String key;

  public S3ByteSink(S3Client s3, String bucket, String key) {
    this.s3 = s3;
    this.bucket = bucket;
    this.key = key;
  }

  @Override
  public S3OutputStream openStream() throws IOException {
    return new S3OutputStream(s3, bucket, key);
  }
}
