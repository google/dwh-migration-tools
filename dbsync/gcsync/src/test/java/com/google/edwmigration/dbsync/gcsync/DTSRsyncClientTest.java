package com.google.edwmigration.dbsync.gcsync;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.OperationCallable;
import com.google.cloud.run.v2.CreateJobRequest;
import com.google.cloud.run.v2.Execution;
import com.google.cloud.run.v2.Job;
import com.google.cloud.run.v2.JobName;
import com.google.cloud.run.v2.JobsClient;
import com.google.cloud.run.v2.RunJobRequest;
import com.google.cloud.storage.Blob;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.edwmigration.dbsync.common.InstructionGenerator;
import com.google.edwmigration.dbsync.storage.gcs.GcsStorage;
import com.google.protobuf.util.Durations;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DTSRsyncClientTest {

  private static final String PROJECT = "dummy-project";
  private static final String TMP_BUCKET = "gs://dummy-tmp-bucket/";
  private static final String TARGET_BUCKET = "gs://dummy-target-bucket/";
  private static final String LOCATION = "us-central1";

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private File sourceDir;
  private File smallCsvFile;
  private File largeCsvFile;
  private File otherFile;

  private GcsStorage mockGcsStorage;
  private InstructionGenerator mockInstructionGenerator;
  private JobsClient mockJobsClient;

  private DTSRsyncClient clientUnderTest;

  // For test simplicity, define a smaller threshold for "rsync" logic
  private static final long RSYNC_SIZE_THRESHOLD = 1024; // e.g. 1KB

  @Before
  public void setUp() throws Exception {
    // Create a local "source" directory with a small file + a large file
    sourceDir = tempFolder.newFolder("sourceDir");

    smallCsvFile = new File(sourceDir, "small.csv");
    Files.asByteSink(smallCsvFile).write("small content".getBytes());

    largeCsvFile = new File(sourceDir, "large.csv");
    // Create a file bigger than the threshold
    byte[] largeBytes = new byte[(int) (RSYNC_SIZE_THRESHOLD + 500)]; // e.g. 1.5KB
    for (int i = 0; i < largeBytes.length; i++) {
      largeBytes[i] = (byte) ('A' + (i % 26));
    }
    Files.asByteSink(largeCsvFile).write(largeBytes);

    otherFile = new File(sourceDir, "other.txt");
    Files.asByteSink(otherFile).write("other content".getBytes());

    // Create mocks
    mockGcsStorage = mock(GcsStorage.class);
    mockInstructionGenerator = mock(InstructionGenerator.class);
    mockJobsClient = mock(JobsClient.class);

    // Now construct the DTSRsyncClient with the mock dependencies
    clientUnderTest =
        new DTSRsyncClient(
            PROJECT,
            TMP_BUCKET,
            TARGET_BUCKET,
            LOCATION,
            sourceDir.getAbsolutePath(),
            Durations.fromSeconds(10),
            mockJobsClient,
            mockGcsStorage,
            mockInstructionGenerator // The key new argument
        );

    // Stub GcsStorage calls:
    //    - "small.csv" does not exist on GCS => returns null => triggers upload
    when(mockGcsStorage.getBlob(eq(TARGET_BUCKET), eq("small.csv"))).thenReturn(null);

    //    - "large.csv" => return a mock Blob (we'll set its MD5 dynamically in each test)
    Blob mockBlobLarge = mock(Blob.class);
    when(mockGcsStorage.getBlob(eq(TARGET_BUCKET), eq("large.csv"))).thenReturn(mockBlobLarge);
    String localMd5Large = generateMd5(largeCsvFile);
    when(mockBlobLarge.getMd5()).thenReturn(localMd5Large);

    when(mockGcsStorage.newByteSource(any())).thenReturn(mock(ByteSource.class));
    ByteSink mockByteSink = mock(ByteSink.class);
    when(mockGcsStorage.newByteSink(any())).thenReturn(mockByteSink);
    when(mockByteSink.openBufferedStream()).thenReturn(mock(BufferedOutputStream.class));
    when(mockByteSink.openStream()).thenReturn(mock(OutputStream.class));

    // Stub out any uploading just to verify calls (optional)
    doNothing().when(mockGcsStorage).uploadFile(any(Path.class), any());

    // --------------------------------------------------------------------------------
    // Setup Mocks for the JobsClient calls that happen in "executeMainOnCloudRun(...)"
    // so they don't actually hit GCP.

    // 1) createJobAsync(...) => returns an OperationFuture<Job, OperationMetadata>
    @SuppressWarnings("unchecked")
    OperationFuture<Job, Job> mockCreateFuture = mock(OperationFuture.class);

    // We'll stub .get() to return a dummy Job so we don't throw an exception
    Job dummyJob =
        Job.newBuilder()
            .setName("projects/dummy-project/locations/us-central1/jobs/dummyJob")
            .build();
    try {
      when(mockCreateFuture.get()).thenReturn(dummyJob);
    } catch (Exception e) {
      // ignored
    }

    @SuppressWarnings("unchecked")
    OperationFuture<Execution, Execution> mockRunFuture = mock(OperationFuture.class);
    Execution dummyExec = Execution.newBuilder().setName("dummyExecution").build();
    try {
      when(mockRunFuture.get()).thenReturn(dummyExec);
    } catch (Exception e) {
      // ignored
    }

    @SuppressWarnings("unchecked")
    OperationFuture<Job, Job> mockDeleteFuture = mock(OperationFuture.class);
    try {
      when(mockDeleteFuture.get()).thenReturn(dummyJob);
    } catch (Exception e) {
      // ignored
    }

    OperationCallable<RunJobRequest, Execution, Execution> operationCallable =
        mock(OperationCallable.class);

    // Now wire them all up
    when(mockJobsClient.createJobAsync(any(CreateJobRequest.class))).thenReturn(mockCreateFuture);
    when(mockJobsClient.runJobOperationCallable()).thenReturn(operationCallable);
    when(operationCallable.futureCall(any(RunJobRequest.class))).thenReturn(mockRunFuture);
    when(mockJobsClient.deleteJobAsync(any(JobName.class))).thenReturn(mockDeleteFuture);
  }

  @After
  public void tearDown() {
    // No special cleanup required; TemporaryFolder is auto-cleaned.
  }

  @Test
  public void testSyncFiles_LargeCsvMd5Matches_NoCloudRun() throws Exception {
    clientUnderTest.syncFiles();

    List<Path> rsyncFiles = getPrivateList(clientUnderTest, "filesToRsync");
    assertFalse(
        "large.csv should NOT be in the rsync list if MD5 matches",
        rsyncFiles.contains(largeCsvFile.toPath()));

    // No job calls
    verify(mockJobsClient, never()).createJobAsync(any(CreateJobRequest.class));
    verify(mockJobsClient.runJobOperationCallable(), never()).futureCall(any(RunJobRequest.class));
    verify(mockJobsClient, never()).deleteJobAsync(any(JobName.class));
  }

  @Test
  public void testSyncFiles_SmallCsvNotOnGcs_UploadsItButNoJob() throws Exception {
    // small.csv not on GCS => we upload it
    // But no job because there's no large file mismatch
    clientUnderTest.syncFiles();

    // check that small file is in "filesToUpload"
    List<Path> uploadFiles = getPrivateList(clientUnderTest, "filesToUpload");
    assertTrue("small.csv should be in upload list", uploadFiles.contains(smallCsvFile.toPath()));

    verify(mockGcsStorage, atLeastOnce()).uploadFile(eq(smallCsvFile.toPath()), any());

    // No Cloud Run job triggered
    verify(mockJobsClient, never()).createJobAsync(any(CreateJobRequest.class));
    verify(mockJobsClient.runJobOperationCallable(), never()).futureCall(any(RunJobRequest.class));
    verify(mockJobsClient, never()).deleteJobAsync(any(JobName.class));
  }

  @Test
  public void testSyncFiles_OtherFileIgnored() throws Exception {
    clientUnderTest.syncFiles();

    List<Path> uploadFiles = getPrivateList(clientUnderTest, "filesToUpload");
    assertFalse("other.txt should NOT be in upload list", uploadFiles.contains(otherFile.toPath()));

    List<Path> rsyncFiles = getPrivateList(clientUnderTest, "filesToRsync");
    assertFalse("other.txt should NOT be in rsync list", rsyncFiles.contains(otherFile.toPath()));

    verify(mockGcsStorage, never()).uploadFile(eq(otherFile.toPath()), any());
    verify(mockJobsClient, never()).createJobAsync(any(CreateJobRequest.class));
  }

  // ----------------------------------------------------------------------------------
  // Helper to reflect into the private list fields (filesToRsync, filesToUpload)
  @SuppressWarnings("unchecked")
  private static List<Path> getPrivateList(DTSRsyncClient client, String fieldName) {
    try {
      java.lang.reflect.Field field = DTSRsyncClient.class.getDeclaredField(fieldName);
      field.setAccessible(true);
      return (List<Path>) field.get(client);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Helper to generate a Base64-encoded MD5 from a local file
  private static String generateMd5(File file) throws IOException {
    byte[] content = Files.toByteArray(file);
    try {
      java.security.MessageDigest digest = java.security.MessageDigest.getInstance("MD5");
      byte[] hash = digest.digest(content);
      return Base64.getEncoder().encodeToString(hash);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}