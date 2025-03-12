package com.google.edwmigration.dbsync.gcsync;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.run.v2.CreateJobRequest;
import com.google.cloud.run.v2.JobsClient;
import com.google.cloud.storage.Blob;
import com.google.common.io.Files;
import com.google.edwmigration.dbsync.common.InstructionGenerator;
import com.google.edwmigration.dbsync.storage.gcs.GcsStorage;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

@SuppressWarnings("unchecked")
public class GcsyncClientTest {

  private static final String PROJECT = "dummy-project";
  private static final String TMP_BUCKET = "gs://dummy-tmp-bucket/";
  private static final String TARGET_BUCKET = "gs://dummy-target-bucket/";
  private static final String LOCATION = "us-central1";

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private File sourceDir;
  private File smallFile;
  private File largeFile;

  private GcsStorage mockGcsStorage;
  private InstructionGenerator mockInstructionGenerator;
  private JobsClient mockJobsClient;

  private GcsyncClient clientUnderTest;

  // For test simplicity, define a smaller threshold for "rsync" logic
  private static final long RSYNC_SIZE_THRESHOLD = Constants.RSYNC_SIZE_THRESHOLD;

  @Before
  public void setUp() throws Exception {
    // Create a local "source" directory with a small file + a large file
    sourceDir = tempFolder.newFolder("sourceDir");

    smallFile = new File(sourceDir, "small.txt");
    Files.asByteSink(smallFile).write("small content".getBytes());

    largeFile = new File(sourceDir, "large.txt");
    // Create a file bigger than the threshold
    byte[] largeBytes = new byte[(int) (RSYNC_SIZE_THRESHOLD + 500)];  // e.g. 1.5KB
    for (int i = 0; i < largeBytes.length; i++) {
      largeBytes[i] = (byte) ('A' + (i % 26));
    }
    Files.asByteSink(largeFile).write(largeBytes);

    // Create mocks
    mockGcsStorage = mock(GcsStorage.class);
    mockInstructionGenerator = mock(InstructionGenerator.class);
    mockJobsClient = mock(JobsClient.class);

    // Now construct the GcsyncClient with the mock dependencies
    clientUnderTest = new GcsyncClient(
        PROJECT,
        TMP_BUCKET,
        TARGET_BUCKET,
        LOCATION,
        mockJobsClient,
        sourceDir.getAbsolutePath(),
        mockGcsStorage,
        mockInstructionGenerator
    );

    // Stub GcsStorage calls:
    //   - If "small.txt" does not exist on GCS => returns null
    when(mockGcsStorage.getBlob(eq(TARGET_BUCKET), eq("small.txt"))).thenReturn(null);
    //   - If "large.txt" exists on GCS => return a mock Blob
    Blob mockBlob = mock(Blob.class);
    when(mockGcsStorage.getBlob(eq(TARGET_BUCKET), eq("large.txt"))).thenReturn(mockBlob);

    // Also stub out any uploading just to verify calls (optional)
    doNothing().when(mockGcsStorage).uploadFile(any(Path.class), any());
  }

  @After
  public void tearDown() {
    // No special cleanup required; TemporaryFolder is auto-cleaned.
  }

  @Test
  public void testSyncFiles_LargeFileRsync_TriggersChecksumAndRunJob()
      throws Exception {
    // Suppose the large file on GCS has an MD5 mismatch
    Blob mockBlob = mockGcsStorage.getBlob(TARGET_BUCKET, "large.txt");
    when(mockBlob.getMd5()).thenReturn("some-other-md5");

    // Now calling syncFiles() should eventually do:
    //  1) Mark large.txt for rsync
    //  2) call computeCheckSum() => executeMainOnCloudRun() => jobsClient calls
    clientUnderTest.syncFiles();

    // verify that large.txt is in "filesToRsync"
    List<Path> rsyncFiles = getPrivateList(clientUnderTest, "filesToRsync");
    assertTrue("large.txt should be in rsync list", rsyncFiles.contains(largeFile.toPath()));

    // We know from the code that computeCheckSum() calls "runCloudRunJob(...)"
    // So let's verify that we used the mockJobsClient to create & run a job:
    verify(mockJobsClient, atLeastOnce()).createJobAsync(any(CreateJobRequest.class));
    verify(mockJobsClient, atLeastOnce()).runJobOperationCallable();
  }

  @Test
  public void testSyncFiles_SmallFileUpload_NoCloudRunJob()
      throws Exception {
    // getBlob(...) for "small.txt" => null => we upload it, but do *not* run Cloud Run job
    clientUnderTest.syncFiles();

    // Check that small.txt is in "filesToUpload"
    List<Path> uploadFiles = getPrivateList(clientUnderTest, "filesToUpload");
    assertTrue("small.txt should be in upload list", uploadFiles.contains(smallFile.toPath()));

    // Because there's no large file mismatch, we do not run any job
    verify(mockJobsClient, never()).createJobAsync(any(CreateJobRequest.class));
    verify(mockJobsClient, never()).runJobOperationCallable();
  }

  @Test
  public void testSyncFiles_LargeFileMatchesMd5_NoCloudRun() throws Exception {
    // If large.txt on GCS has the same MD5 => no rsync => no job
    Blob mockBlob = mockGcsStorage.getBlob(TARGET_BUCKET, "large.txt");
    String localMd5 = generateMd5(largeFile);
    when(mockBlob.getMd5()).thenReturn(localMd5);

    clientUnderTest.syncFiles();

    List<Path> rsyncFiles = getPrivateList(clientUnderTest, "filesToRsync");
    assertFalse("large.txt should not be rsync-ed", rsyncFiles.contains(largeFile.toPath()));

    verify(mockJobsClient, never()).createJobAsync(any(CreateJobRequest.class));
    verify(mockJobsClient, never()).runJobOperationCallable();
  }

  // ----------------------------------------------------------------------------------
  // Helper to reflect into the private list fields (filesToRsync, filesToUpload)
  @SuppressWarnings("unchecked")
  private static List<Path> getPrivateList(GcsyncClient client, String fieldName) {
    try {
      java.lang.reflect.Field field = GcsyncClient.class.getDeclaredField(fieldName);
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
