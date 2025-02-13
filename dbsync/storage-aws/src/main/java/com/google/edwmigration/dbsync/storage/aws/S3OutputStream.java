package com.google.edwmigration.dbsync.storage.aws;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.checkerframework.checker.nullness.qual.EnsuresNonNull;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.internal.util.Mimetype;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

// Based on https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/examples-s3-objects.html
// With bits loosely derived from https://gist.github.com/jcputney/b5daeb86a1c0696859da2a0c3b466327
// Via https://stackoverflow.com/questions/60212728/how-to-create-a-java-outputstream-for-an-s3-object-and-write-value-to-it
// With very limited help from https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-upload-object.html
// Then mucked about and "optimized" by me, until it reached its current (probably broken) state.
public class S3OutputStream extends OutputStream {

  /**
   * Default chunk size is 10MB; minimum is 5Mb, maximum is large.
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
   */
  private static final int BUFFER_SIZE = 10 * 1024 * 1024;

  private final S3Client s3Client;
  /**
   * The bucket-name on Amazon S3
   */
  private final String bucket;
  /**
   * The key within the bucket
   */
  private final String key;

  /**
   * The temporary buffer used for storing the chunks
   */
  private final byte[] buf = new byte[BUFFER_SIZE];
  /**
   * The position in the buffer
   */
  private int position = 0;

  /**
   * The unique id for this upload
   */
  private String uploadId;
  /**
   * Collection of the etags for the parts that have been uploaded
   */
  private final List<String> etags = new ArrayList<>();

  /**
   * Indicates whether the stream is still open / valid
   */
  private boolean open = true;

  /**
   * Creates a new S3 OutputStream
   *
   * @param s3Client the AmazonS3 client
   * @param bucket name of the bucket
   * @param key path within the bucket
   */
  public S3OutputStream(S3Client s3Client, String bucket, String key) {
    this.s3Client = s3Client;
    this.bucket = bucket;
    this.key = key;
  }

  private void assertOpen() throws IOException {
    if (!open) {
      throw new IOException("S3 output stream is already closed");
    }
  }

  public void cancel() throws IOException {
    try {
      open = false;
      if (uploadId != null) {
        s3Client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
            .bucket(bucket)
            .key(key)
            .uploadId(uploadId)
            .build());
      }
    } catch (SdkException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void write(int b) throws IOException {
    assertOpen();
    if (position >= buf.length) {
      flushBufferAndRewind();
    }
    buf[position++] = (byte) b;
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    assertOpen();
    int size;
    while (len > (size = buf.length - position)) {
      flushHeader();
      if (position > 0) {
        // We might have a buffer which contains less than 5Mb (minimum part size).
        // So we bulk it up here, so we can upload it.
        System.arraycopy(b, off, buf, position, size);
        position += size;
        flushPart(buf, 0, position);
        position = 0;
      } else {
        // There's no need to copy the source data into our byte array. size == BUFFER_SIZE.
        // We might be able to push up the rest of the byte[] here and just return.
        // The API says the minimum part size is 5Mb, so we have to copy some of it.
        // But here, we know the 'remainder' is more than 10Mb.
        flushPart(b, off, size);
      }
      off += size;
      len -= size;
    }
    System.arraycopy(b, off, buf, position, len);
    position += len;
  }

  // Factored out to give a choice.
  private static RequestBody newRequestBody(byte[] buf, int off, int len) {
    if (true) {
      // This is "faster".
      return RequestBody.fromContentProvider(
          () -> new ByteArrayInputStream(buf, off, len),
          len,
          Mimetype.MIMETYPE_OCTET_STREAM);
    } else {
      // This is "safer", but I don't think in any way that matters.
      return RequestBody.fromInputStream(
          new ByteArrayInputStream(buf, off, len),
          len);
    }
  }

  @EnsuresNonNull("uploadId")
  private void flushHeader() throws IOException {
    try {
      if (uploadId == null) {
        CreateMultipartUploadRequest uploadRequest = CreateMultipartUploadRequest.builder()
            .bucket(bucket)
            .key(key)
            .build();
        CreateMultipartUploadResponse multipartUpload = s3Client.createMultipartUpload(
            uploadRequest);
        uploadId = multipartUpload.uploadId();
      }
    } catch (NoSuchBucketException e) {
      throw S3Utils.newFileNotFoundException("No such bucket s3://" + bucket + "/", e);
    } catch (SdkException e) {
      throw new IOException(e);
    }
  }

  @RequiresNonNull("uploadId")
  private void flushPart(byte[] buf, int off, int len) throws IOException {
    try {
      UploadPartRequest uploadRequest = UploadPartRequest.builder()
          .bucket(bucket)
          .key(key)
          .uploadId(uploadId)
          .partNumber(etags.size() + 1)
          .contentLength((long) len)
          .build();
      RequestBody requestBody = newRequestBody(buf, off, len);
      UploadPartResponse uploadPartResponse = s3Client.uploadPart(uploadRequest, requestBody);
      etags.add(uploadPartResponse.eTag());
    } catch (SdkException e) {
      throw new IOException(e);
    }
  }

  @EnsuresNonNull("uploadId")
  private void flushBufferAndRewind() throws IOException {
    flushHeader();
    flushPart(buf, 0, position);
    position = 0;
  }

  @Override
  public void close() throws IOException {
    try {
      if (!open) {
        return;
      }
      open = false;

      if (uploadId != null) {
        if (position > 0) {
          flushPart(buf, 0, position);
        }

        CompletedPart[] completedParts = new CompletedPart[etags.size()];
        for (int i = 0; i < etags.size(); i++) {
          completedParts[i] = CompletedPart.builder()
              .eTag(etags.get(i))
              .partNumber(i + 1)
              .build();
        }

        CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
            .parts(completedParts)
            .build();
        CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
            .bucket(bucket)
            .key(key)
            .uploadId(uploadId)
            .multipartUpload(completedMultipartUpload)
            .build();
        s3Client.completeMultipartUpload(completeMultipartUploadRequest);
      } else {
        PutObjectRequest putRequest = PutObjectRequest.builder()
            .bucket(bucket)
            .key(key)
            .contentLength((long) position)
            .build();

        RequestBody requestBody = newRequestBody(buf, 0, position);
        s3Client.putObject(putRequest, requestBody);
      }
    } catch (NoSuchBucketException e) {
      throw S3Utils.newFileNotFoundException("No such bucket s3://" + bucket + "/", e);
    } catch (SdkException e) {
      throw new IOException(e);
    }
  }

}
