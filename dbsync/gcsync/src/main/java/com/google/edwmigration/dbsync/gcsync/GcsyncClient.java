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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsyncClient {

  private final String project;
  private final String tmpBucket;
  private final String targetBucket;

  private final String location;

  private final String sourceDirectory;

  private final Duration cloudRunTaskTimeout;

  private final GcsStorage gcsStorage;
  private final List<Path> filesToUpload;

  private final List<Path> filesToRsync;

  private final Map<Path, String> fileToMd5;

  private final int numOfConcurrentTasks;
  private static final Logger logger = LoggerFactory.getLogger(GcsyncClient.class);

  public GcsyncClient(
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
    filesToRsync = new ArrayList<>();
    filesToUpload = new ArrayList<>();
    fileToMd5 = new HashMap<>();
  }

  public void syncFiles() throws IOException, URISyntaxException, InterruptedException {
    // Scan the files in the path the program is running, get a list of files to rsync or upload,
    // and upload that list to gcs.
    scanFiles();

    logger.info("Files to rsync: " + filesToRsync);
    logger.info("Files to upload: " + filesToUpload);
    if (!filesToRsync.isEmpty()) {
      uploadJar();
    }
    ExecutorService executorService = Executors.newFixedThreadPool(numOfConcurrentTasks);
    executorService.invokeAll(createTasks());
    executorService.shutdown();

    uploadFiles();
  }

  private List<GcsyncTask> createTasks() throws IOException {
    List<List<Path>> filesToRsyncBuckets =
        distributeFileIntoBuckets(filesToRsync, numOfConcurrentTasks);
    List<GcsyncTask> tasks = new ArrayList<>();

    for (List<Path> filesToRsync : filesToRsyncBuckets) {
      tasks.add(
          new GcsyncTask(
              filesToRsync,
              targetBucket,
              tmpBucket,
              project,
              location,
              cloudRunTaskTimeout,
              gcsStorage,
              JobsClient.create(jobsSettings(cloudRunTaskTimeout)),
              new InstructionGenerator(Constants.BLOCK_SIZE),
              new HashMap<>(fileToMd5)));
    }

    return tasks;
  }

  private static List<List<Path>> distributeFileIntoBuckets(List<Path> files, int size) {
    Queue<Path> queueOfFiles = new ArrayDeque<>(files);

    PriorityQueue<List<Path>> bucketsByTotalSize = new PriorityQueue<>(new FileListComparator());
    for (int i = 0; i < size; i++) {
      bucketsByTotalSize.offer(new ArrayList<>());
    }

    // We use a greedy approach to distribute files by always sending the file to the bucket that
    // currently have the smallest total size (summed by files in the bucket)
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

  private void scanFiles() throws IOException, URISyntaxException {
    Path currentDirectory = Paths.get(sourceDirectory);
    checkNotNull(currentDirectory);

    try (DirectoryStream<Path> stream = Files.newDirectoryStream(currentDirectory)) {
      for (Path path : stream) {
        if (path.getFileName().toString().equals(Constants.JAR_FILE_NAME)) {
          // Don't sync the jar to target bucket
          continue;
        }
        if (Files.isDirectory(path)) {
          // We don't support nested scanning for now
          continue;
        }
        if (Files.size(path) < Constants.RSYNC_SIZE_THRESHOLD) {
          // If file is small, we just upload it.
          filesToUpload.add(path);
        } else {
          Blob blob = gcsStorage.getBlob(targetBucket, path.getFileName().toString());
          if (blob == null) {
            // File doesn't exist on gcs, we have to upload it.
            filesToUpload.add(path);
          } else {
            String gcsMd5 = blob.getMd5();
            String localMd5 = generateMd5(path);
            fileToMd5.put(path, localMd5);

            if (gcsMd5 == null || !gcsMd5.equals(localMd5)) {
              filesToRsync.add(path);
            }
          }
        }
      }
    }
  }

  private void uploadJar() throws URISyntaxException, IOException {
    Blob blob = gcsStorage.getBlob(tmpBucket, Constants.JAR_FILE_NAME);
    Path sourceJar = Paths.get(Constants.JAR_FILE_NAME);
    if (sourceJar == null) {
      throw new IllegalArgumentException("The jar file has been deleted");
    }
    if (blob == null || !blob.getMd5().equals(generateMd5(sourceJar))) {
      gcsStorage.uploadFile(sourceJar, new URI(tmpBucket).resolve(Constants.JAR_FILE_NAME));
    }
  }

  private static String generateMd5(Path file) throws IOException {
    return Base64.getEncoder()
        .encodeToString(
            com.google.common.io.Files.asByteSource(file.toFile()).hash(Hashing.md5()).asBytes());
  }

  private void uploadFiles() throws URISyntaxException, IOException {
    for (Path file : filesToUpload) {
      gcsStorage.uploadFile(file, new URI(targetBucket).resolve(file.getFileName().toString()));
    }
  }

  private static JobsSettings jobsSettings(Duration jobTimeout) throws IOException {
    JobsSettings.Builder builder = JobsSettings.newBuilder();
    builder
        .runJobOperationSettings()
        .setPollingAlgorithm(
            OperationTimedPollAlgorithm.create(
                RetrySettings.newBuilder()
                    .setTotalTimeoutDuration(
                        java.time.Duration.ofSeconds(
                            jobTimeout.getSeconds() + Constants.EXTRA_POLLING_TIMEOUT.getSeconds()))
                    .setInitialRetryDelayDuration(java.time.Duration.ofSeconds(1))
                    .setRetryDelayMultiplier(1.5)
                    .setMaxRetryDelayDuration(java.time.Duration.ofSeconds(45))
                    .build()));
    return builder.build();
  }
}
