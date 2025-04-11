package com.google.edwmigration.dbsync.storage.gcs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.CopyRequest;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import org.checkerframework.checker.nullness.qual.NonNull;

// https://www.baeldung.com/java-google-cloud-storage
public class GcsStorage {

  public static final String SCHEME = "gs";

  private final Storage storage;

  public GcsStorage(String projectId) {
    this.storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
  }

  public Blob getBlob(String bucket, String objectName) throws URISyntaxException {
    return storage.get(BlobId.fromGsUtilUri(new URI(bucket).resolve(objectName).toString()));
  }

  public Blob getBlob(URI uri) {
    return storage.get(BlobId.fromGsUtilUri(uri.toString()));
  }

  public @NonNull ByteSource newByteSource(URI uri) {
    Preconditions.checkArgument(SCHEME.equals(uri.getScheme()));
    return new GcsByteSource(storage, BlobId.fromGsUtilUri(uri.toString()));
  }

  public @NonNull ByteSink newByteSink(URI uri) {
    Preconditions.checkArgument(SCHEME.equals(uri.getScheme()));
    return new GcsByteSink(storage, BlobId.fromGsUtilUri(uri.toString()));
  }

  public void uploadFile(Path sourceFile, URI target) throws IOException {
    storage.createFrom(
        BlobInfo.newBuilder(BlobId.fromGsUtilUri(target.toString())).build(), sourceFile);
  }

  public boolean delete(URI file) {
    return storage.delete(BlobId.fromGsUtilUri(file.toString()));
  }

  public void copyFile(URI sourceUri, URI targetUri) {
    CopyWriter copyWriter =
        storage.copy(
            CopyRequest.newBuilder()
                .setSource(BlobId.fromGsUtilUri(sourceUri.toString()))
                .setTarget(BlobId.fromGsUtilUri(targetUri.toString()))
                .build());
    copyWriter.getResult();
  }
}
