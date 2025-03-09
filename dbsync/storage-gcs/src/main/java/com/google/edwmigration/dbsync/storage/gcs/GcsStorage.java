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
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

// https://www.baeldung.com/java-google-cloud-storage
public class GcsStorage {

  public static final String SCHEME = "gs";

  private final Storage storage;

  public GcsStorage(String projectId) {
    this.storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
  }

  public @NonNull ByteSource newByteSource(URI uri) {
    Preconditions.checkArgument(SCHEME.equals(uri.getScheme()));
    return new GcsByteSource(storage, BlobId.fromGsUtilUri(uri.toString()));
  }

  public @NonNull ByteSink newByteSink(URI uri) {
    Preconditions.checkArgument(SCHEME.equals(uri.getScheme()));
    return new GcsByteSink(storage, BlobId.fromGsUtilUri(uri.toString()));
  }

  public void uploadFile(Path filepath, URI target) throws IOException {
    storage.createFrom(
        BlobInfo.newBuilder(BlobId.fromGsUtilUri(target.toString())).build(), filepath);
  }

  public void copyFile(URI sourceUri, URI targetUri) {
    CopyWriter copyWriter =
        storage.copy(
            CopyRequest.newBuilder()
                .setSource(BlobId.fromGsUtilUri(sourceUri.toString()))
                .setTarget(BlobId.fromGsUtilUri(targetUri.toString()))
                .build());
    // We want to block until copy is completed
    copyWriter.getResult();
  }

  public boolean deleteFile(URI fileUri) {
    // return if file got deleted or not and let caller decide how to handle this
    boolean isDeleted = storage.delete(BlobId.fromGsUtilUri(fileUri.toString()));
    if (!isDeleted) {
      Logger.getLogger("GcsStorage")
          .log(Level.WARNING, "File to delete was not found, fileUri: %s", fileUri);
    }
    return isDeleted;
  }

  public boolean checkFileExists(String fileGsUtilUri) {
    Blob blob = storage.get(BlobId.fromGsUtilUri(fileGsUtilUri));
    return blob != null && blob.exists();
  }
}
