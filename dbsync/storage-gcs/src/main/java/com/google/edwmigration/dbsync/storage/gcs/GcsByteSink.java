package com.google.edwmigration.dbsync.storage.gcs;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.common.io.ByteSink;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;

public class GcsByteSink extends ByteSink {

    private final Storage storage;
    private final BlobId blobId;

    public GcsByteSink(Storage storage, BlobId blobId) {
        this.storage = storage;
        this.blobId = blobId;
    }

    @Override
    public OutputStream openStream() throws IOException {
        WriteChannel channel = storage.writer(BlobInfo.newBuilder(blobId).build());
        return Channels.newOutputStream(channel);
    }
}
