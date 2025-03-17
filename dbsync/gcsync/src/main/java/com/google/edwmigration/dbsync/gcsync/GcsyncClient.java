package com.google.edwmigration.dbsync.gcsync;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.run.v2.Container;
import com.google.cloud.run.v2.CreateJobRequest;
import com.google.cloud.run.v2.Execution;
import com.google.cloud.run.v2.ExecutionTemplate;
import com.google.cloud.run.v2.Job;
import com.google.cloud.run.v2.JobName;
import com.google.cloud.run.v2.JobsClient;
import com.google.cloud.run.v2.RunJobRequest;
import com.google.cloud.run.v2.TaskTemplate;
import com.google.cloud.storage.Blob;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteSource;
import com.google.edwmigration.dbsync.common.InstructionGenerator;
import com.google.edwmigration.dbsync.proto.Checksum;
import com.google.edwmigration.dbsync.storage.gcs.GcsStorage;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GcsyncClient {

  private final String project;
  private final String tmpBucket;
  private final String targetBucket;

  private final String location;

  private final JobsClient jobsClient;

  private final String sourceDirectory;

  private final GcsStorage gcsStorage;
  private List<Path> filesToUpload;

  private List<Path> filesToRsync;

  private final InstructionGenerator instructionGenerator;

  private static final Logger logger = Logger.getLogger("rsync");

  public GcsyncClient(String project, String tmpBucket, String targetBucket,
      String location, String sourceDirectory, JobsClient jobsClient, GcsStorage gcsStorage,
      InstructionGenerator instructionGenerator) {
    this.project = project;
    this.tmpBucket = tmpBucket;
    this.targetBucket = targetBucket;
    this.location = location;
    this.jobsClient = jobsClient;
    this.gcsStorage = gcsStorage;
    this.sourceDirectory = sourceDirectory;
    this.instructionGenerator = instructionGenerator;
    filesToRsync = new ArrayList<>();
    filesToUpload = new ArrayList<>();
  }

  public void syncFiles()
      throws IOException, URISyntaxException, ExecutionException, InterruptedException {
    // Scan the files in the path the program is running, get a list of files to rsync or upload,
    // and upload that list to gcs.
    scanFiles();

    logger.log(Level.INFO, "Files to rsync: " + filesToRsync);
    logger.log(Level.INFO, "Files to upload: " + filesToUpload);
    if (!filesToRsync.isEmpty()) {
      // Compute checksums for files to rsync.
      computeCheckSum();
      // Download checksum files to local storage.
      downloadCheckSumFiles();
      // Send instruction files to gcs.
      sendRsyncInstructions();
      // Reconstruct files on gcs using instructions.
      reconStructFiles();
    }
    // Upload files that cannot be rsynced and small files.
    uploadRemainingFiles();
  }

  private void scanFiles() throws IOException, URISyntaxException {
    Path currentDirectory = Paths.get(sourceDirectory);
    Preconditions.checkNotNull(currentDirectory);

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
            if (!gcsMd5.equals(generateMd5(path))) {
              filesToRsync.add(path);
            }
          }
        }
      }
    }
  }

  private void computeCheckSum()
      throws URISyntaxException, IOException, ExecutionException, InterruptedException {
    uploadFilesToRsyncList();
    uploadJar();

    executeMainOnCloudRun(Constants.GENERATE_CHECK_SUM_MAIN);
  }

  private void uploadJar() throws URISyntaxException, IOException {
    Blob blob = gcsStorage.getBlob(tmpBucket, Constants.JAR_FILE_NAME);
    Path sourceJar = Paths.get(Constants.JAR_FILE_NAME);
    if (sourceJar == null) {
      throw new IllegalArgumentException("The jar file has been deleted");
    }
    if (blob == null || !blob.getMd5().equals(generateMd5(sourceJar))) {
      gcsStorage.uploadFile(sourceJar,
          new URI(tmpBucket).resolve(Constants.JAR_FILE_NAME));
    }
  }

  private void uploadFilesToRsyncList() throws URISyntaxException, IOException {
    Path tempFile = Files.createTempFile("filesToRsyncTmp", ".txt");
    try (BufferedWriter writer = Files.newBufferedWriter(tempFile)) {
      for (Path file : filesToRsync) {
        writer.write(file.getFileName().toString());
        writer.newLine();
      }
    }

    gcsStorage.uploadFile(tempFile,
        new URI(tmpBucket).resolve(Constants.FILES_TO_RSYNC_FILE_NAME));

    Files.deleteIfExists(tempFile);
  }

  private void executeMainOnCloudRun(String mainClassPath)
      throws URISyntaxException, IOException, ExecutionException, InterruptedException {
    String downloadJarCommand = String.format("gcloud storage cp %s .",
        new URI(tmpBucket).resolve(Constants.JAR_FILE_NAME));

    String command = String.format(
        "java -cp %s "
            + String.format("%s ", mainClassPath)
            + "--project %s "
            + "--tmp_bucket %s "
            + "--target_bucket %s", Constants.JAR_FILE_NAME, project, tmpBucket, targetBucket);

    runCloudRunJob(String.format("%s && %s", downloadJarCommand, command), project);
  }

  private void downloadCheckSumFiles() throws URISyntaxException, IOException {
    for (Path file : filesToRsync) {
      String checksumFileName = Util.getCheckSumFileName(file.getFileName().toString());
      ByteSource byteSource = gcsStorage.newByteSource(
          new URI(tmpBucket).resolve(checksumFileName));
      byteSource.copyTo(
          com.google.common.io.Files.asByteSink(getTemporaryCheckSumFileName(file).toFile()));
    }
  }

  private void sendRsyncInstructions() throws IOException, URISyntaxException {
    for (Path file : filesToRsync) {
      Path tmpCheckSumFile = getTemporaryCheckSumFileName(file);
      try (OutputStream instructionSink = gcsStorage.newByteSink(
              new URI(tmpBucket).resolve(Util.getInstructionFileName(file.getFileName().toString())))
          .openBufferedStream()) {
        try (InputStream inputStream = Files.newInputStream(tmpCheckSumFile)) {
          List<Checksum> checksums = getChecksumsFromFile(inputStream);
          ByteSource fileInput = com.google.common.io.Files.asByteSource(file.toFile());

          instructionGenerator.generate(instruction -> {
            try {
              instruction.writeDelimitedTo(instructionSink);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }, fileInput, checksums);
        }
      }

      Files.deleteIfExists(tmpCheckSumFile);
    }
  }

  private static Path getTemporaryCheckSumFileName(Path file) {
    return file.resolveSibling(
        String.format("%s.%s", file.getFileName(), Constants.CHECK_SUM_FILE_SUFFIX));
  }

  private void reconStructFiles()
      throws IOException, ExecutionException, InterruptedException, URISyntaxException {
    executeMainOnCloudRun(Constants.RECONSTRUCT_FILE_MAIN);
  }

  private void uploadRemainingFiles() throws URISyntaxException, IOException {
    for (Path file : filesToUpload) {
      gcsStorage.uploadFile(file, new URI(targetBucket).resolve(file.getFileName().toString()));
    }
  }

  private void runCloudRunJob(String command, String project)
      throws ExecutionException, InterruptedException {
    Job job = Job.newBuilder().setTemplate(ExecutionTemplate.newBuilder().setTemplate(
        TaskTemplate.newBuilder().addContainers(
            Container.newBuilder().setImage("docker.io/getbamba/google-cloud-sdk-java:latest")
                .addCommand("/bin/sh")
                .addAllArgs(Arrays.asList("-c", command)).build()).build()).build()).build();

    String jobId = String.format("gcsync-cloudrun-%s", UUID.randomUUID());
    CreateJobRequest createJobRequest = CreateJobRequest.newBuilder()
        .setParent(String.format("projects/%s/locations/%s", project, location))
        .setJobId(jobId).setJob(job).build();

    jobsClient.createJobAsync(createJobRequest).get();
    JobName jobName = JobName.of(project, location, jobId);
    OperationFuture<Execution, Execution> future = jobsClient.runJobOperationCallable()
        .futureCall(RunJobRequest.newBuilder().setName(jobName.toString()).build());

    // Block until the execution of the job completes, this throws if the job fails.
    future.get();

    // Delete the job to prevent it from spamming cloud jobs list
    jobsClient.deleteJobAsync(jobName).get();
  }

  private static List<Checksum> getChecksumsFromFile(InputStream inputStream) throws IOException {
    List<Checksum> checksums = new ArrayList<>();
    Checksum checksum;
    while ((checksum = Checksum.parseDelimitedFrom(inputStream)) != null) {
      checksums.add(checksum);
    }
    inputStream.close();

    return checksums;
  }

  private static String generateMd5(Path file) throws IOException {
    return Base64.getEncoder().encodeToString(
        com.google.common.io.Files.asByteSource(file.toFile()).hash(Hashing.md5()).asBytes());
  }
}
