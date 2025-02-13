package com.google.edwmigration.dbsync.storage.gcs;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import org.checkerframework.checker.nullness.qual.NonNull;

// https://www.baeldung.com/java-google-cloud-storage
public class GcsStorage {

    public static final String SCHEME = "gs";

    private final String projectId;
    private final Storage storage;

    public GcsStorage(String projectId) {
        // Get today's date at midnight
        LocalDate todayLocal = LocalDate.now();
        ZonedDateTime todayStartOfDay = todayLocal.atStartOfDay(ZoneId.systemDefault());
        Date todayDate = Date.from(todayStartOfDay.toInstant());


        // Calculate tomorrow's date (24 hours later)
        long tomorrowMillis = todayDate.getTime() + (24 * 60 * 60 * 1000); // Milliseconds in a day
        Date tomorrowDate = new Date(tomorrowMillis);
        this.projectId = projectId;
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
        storage.createFrom(BlobInfo.newBuilder(BlobId.fromGsUtilUri(target.toString())).build(), filepath);
    }

}
