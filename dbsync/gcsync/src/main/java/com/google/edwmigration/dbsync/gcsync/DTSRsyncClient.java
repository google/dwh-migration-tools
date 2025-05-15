package com.google.edwmigration.dbsync.gcsync;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.gax.longrunning.OperationTimedPollAlgorithm;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.run.v2.JobsClient;
import com.google.cloud.run.v2.JobsSettings;
import com.google.cloud.storage.Blob;
import com.google.common.hash.Hashing;
import com.google.edwmigration.dbsync.common.InstructionGenerator;
import com.google.edwmigration.dbsync.storage.gcs.GcsStorage;
import com.google.protobuf.Duration;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DTSRsyncClient {

  private final String project;
  private final String tmpBucket;
  private final String targetBucket;
  private final String location;
  private final String sourceDirectory;
  private final Duration cloudRunTaskTimeout;
  private final GcsStorage gcsStorage;
  private final int numOfConcurrentTasks;
  private static final Logger logger = Logger.getLogger("Data Migration Agent");

  private final List<Path> filesToUpload = new ArrayList<>();
  private final List<Path> filesToRsync = new ArrayList<>();
  private final Map<Path, String> fileToMd5 = new HashMap<>();

  public DTSRsyncClient(
      String project,
      String tmpBucket,
      String targetBucket,
      String location,
      String sourceDirectory,
      int numOfConcurrentTasks,
      Duration cloudRunTaskTimeout,
      GcsStorage gcsStorage) {
    this.project = project;
    this.tmpBucket = tmpBucket;
    this.targetBucket = targetBucket;
    this.location = location;
    this.cloudRunTaskTimeout = cloudRunTaskTimeout;
    this.gcsStorage = gcsStorage;
    this.sourceDirectory = sourceDirectory;
    this.numOfConcurrentTasks = numOfConcurrentTasks;
  }

  public void syncFiles()
      throws IOException, URISyntaxException, InterruptedException, ExecutionException {
    // Scan the files in the path the program is running, get a list of files to rsync or upload.
    scanFiles();

    logger.log(Level.INFO, "Files to rsync: " + filesToRsync);
    logger.log(Level.INFO, "Files to upload: " + filesToUpload);

    if (!filesToRsync.isEmpty()) {
      // Upload the JAR file required by Cloud Run jobs.
      uploadJar();

      // Create and execute GcsyncTasks in parallel.
      ExecutorService executorService = Executors.newFixedThreadPool(numOfConcurrentTasks);
      executorService.invokeAll(createRsyncTasks());
      executorService.shutdown();
    }

    // Upload files that cannot be rsynced and small files.
    uploadRemainingFiles();
  }

  private void scanFiles() throws IOException, URISyntaxException {
    final Path currentDirectory = Paths.get(this.sourceDirectory);
    checkNotNull(currentDirectory);
    scanDirectoryRecursive(currentDirectory);
  }

  public void scanDirectoryRecursive(final Path directory) throws IOException, URISyntaxException {
    try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory)) {
      for (Path entry : directoryStream) {
        String fileName = entry.getFileName().toString();
        logger.log(Level.FINE, "Found entry: {0}", entry);

        if (Files.isDirectory(entry, LinkOption.NOFOLLOW_LINKS)) {
          scanDirectoryRecursive(entry); // Recursive call for subdirectory
        } else {
          Pattern filePattern = Pattern.compile(".*\\.csv.*");
          Matcher matcher = filePattern.matcher(fileName.toLowerCase());
          if (matcher.matches()) {
            processFile(entry, fileName);
          }
        }
      }
    } catch (IOException e) {
      logger.log(
          Level.SEVERE,
          "Exception occurred while scanning directory: {0}. Exception: {1}",
          new Object[] {directory.toString(), e.toString()});
      throw e;
    }
  }

  private void processFile(Path filePath, String fileName) throws IOException, URISyntaxException {
    long fileSize = Files.size(filePath);
    if (fileSize < 0) {
      this.filesToUpload.add(filePath);
    } else {
      Blob existingBlob = this.gcsStorage.getBlob(this.targetBucket, fileName);
      if (existingBlob == null) {
        this.filesToUpload.add(filePath);
      } else {
        String gcsMd5 = existingBlob.getMd5();
        String localMd5 = generateMd5(filePath);
        fileToMd5.put(filePath, localMd5);

        if (!gcsMd5.equals(localMd5)) {
          this.filesToRsync.add(filePath);
        }
      }
    }
  }

  private List<GcsyncTask> createRsyncTasks() throws IOException {
    List<List<Path>> filesToRsyncBuckets =
        distributeFileIntoBuckets(filesToRsync, numOfConcurrentTasks);
    List<GcsyncTask> tasks = new ArrayList<>();
    JobsClient jobsClient = createJobsClient();
    InstructionGenerator instructionGenerator = new InstructionGenerator(Constants.BLOCK_SIZE);

    for (List<Path> files : filesToRsyncBuckets) {
      tasks.add(
          new GcsyncTask(
              files,
              targetBucket,
              tmpBucket,
              project,
              location,
              cloudRunTaskTimeout,
              gcsStorage,
              jobsClient,
              instructionGenerator,
              new HashMap<>(fileToMd5))); // Pass a copy
    }
    return tasks;
  }

  private static List<List<Path>> distributeFileIntoBuckets(List<Path> files, int size) {
    Queue<Path> queueOfFiles = new ArrayDeque<>(files);
    PriorityQueue<List<Path>> bucketsByTotalSize = new PriorityQueue<>(new FileListComparator());
    for (int i = 0; i < size; i++) {
      bucketsByTotalSize.offer(new ArrayList<>());
    }

    while (!queueOfFiles.isEmpty()) {
      Path file = queueOfFiles.poll();
      List<Path> bucket = bucketsByTotalSize.poll();
      bucket.add(file);
      bucketsByTotalSize.offer(bucket);
    }
    return new ArrayList<>(bucketsByTotalSize);
  }

  /** A comparator that sorts a list of files by its total size. */
  private static class FileListComparator implements Comparator<List<Path>> {
    @Override
    public int compare(List<Path> o1, List<Path> o2) {
      try {
        long size1 = 0;
        long size2 = 0;
        for (Path file : o1) {
          size1 += Files.size(file);
        }
        for (Path file : o2) {
          size2 += Files.size(file);
        }
        return Long.compare(size1, size2);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void uploadJar() throws URISyntaxException, IOException {
    String resourceName = "gcsync-1.0.0-all.jar";
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream(resourceName);

    if (inputStream == null) {
      throw new IllegalArgumentException("The JAR resource not found: " + resourceName);
    }

    // Create a temporary file to work with the InputStream
    Path tempFile = Files.createTempFile("gcsync-all", ".jar");
    Files.copy(inputStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
    inputStream.close();

    String currentMd5 = generateMd5(tempFile);
    Blob blob = gcsStorage.getBlob(tmpBucket, Constants.JAR_FILE_NAME);

    if (blob == null || (blob.getMd5() != null && !blob.getMd5().equals(currentMd5))) {
      gcsStorage.uploadFile(tempFile, new URI(tmpBucket).resolve(Constants.JAR_FILE_NAME));
    }

    // Clean up the temporary file
    Files.deleteIfExists(tempFile);
  }

  private void uploadRemainingFiles() throws URISyntaxException, IOException {
    for (Path file : filesToUpload) {
      gcsStorage.uploadFile(file, new URI(targetBucket).resolve(file.getFileName().toString()));
    }
  }

  private static String generateMd5(Path file) throws IOException {
    return Base64.getEncoder()
        .encodeToString(
            com.google.common.io.Files.asByteSource(file.toFile()).hash(Hashing.md5()).asBytes());
  }

  private JobsClient createJobsClient() throws IOException {
    JobsSettings.Builder builder = JobsSettings.newBuilder();
    builder
        .runJobOperationSettings()
        .setPollingAlgorithm(
            OperationTimedPollAlgorithm.create(
                RetrySettings.newBuilder()
                    .setTotalTimeoutDuration(
                        java.time.Duration.ofSeconds(
                            cloudRunTaskTimeout.getSeconds()
                                + Constants.EXTRA_POLLING_TIMEOUT.getSeconds()))
                    .setInitialRetryDelayDuration(java.time.Duration.ofSeconds(1))
                    .setRetryDelayMultiplier(1.5)
                    .setMaxRetryDelayDuration(java.time.Duration.ofSeconds(45))
                    .build()));
    return JobsClient.create(builder.build());
  }
}
