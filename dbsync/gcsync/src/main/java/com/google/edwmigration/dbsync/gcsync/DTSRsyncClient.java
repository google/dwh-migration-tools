package com.google.edwmigration.dbsync.gcsync;

import static com.google.common.base.Preconditions.checkNotNull;

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
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.edwmigration.dbsync.common.InstructionGenerator;
import com.google.edwmigration.dbsync.storage.gcs.GcsStorage;
import com.google.protobuf.Duration;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DTSRsyncClient {

  private final String project;
  private final String tmpBucket;
  private final String targetBucket;
  private final String location;
  private final JobsClient jobsClient;
  private final String sourceDirectory;
  private final Duration cloudRunTaskTimeout;
  private final GcsStorage gcsStorage;
  private final List<Path> filesToUpload;
  private final List<Path> filesToRsync;
  private final Map<Path, String> fileToMd5;
  private final InstructionGenerator instructionGenerator;
  private static final Logger logger = Logger.getLogger("Data Migration Agent");

  public DTSRsyncClient(
      String project,
      String tmpBucket,
      String targetBucket,
      String location,
      String sourceDirectory,
      Duration cloudRunTaskTimeout,
      JobsClient jobsClient,
      GcsStorage gcsStorage,
      InstructionGenerator instructionGenerator) {
    this.project = project;
    this.tmpBucket = tmpBucket;
    this.targetBucket = targetBucket;
    this.location = location;
    this.cloudRunTaskTimeout = cloudRunTaskTimeout;
    this.jobsClient = jobsClient;
    this.gcsStorage = gcsStorage;
    this.sourceDirectory = sourceDirectory;
    this.instructionGenerator = instructionGenerator;
    this.filesToRsync = new ArrayList<>();
    this.filesToUpload = new ArrayList<>();
    this.fileToMd5 = new HashMap<>();
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
      // Send instruction files to gcs.
      sendRsyncInstructions();
      // Reconstruct files on gcs using instructions.
      reconStructFiles();
    }
    // Upload files that cannot be rsynced and small files.
    uploadRemainingFiles();
  }

  private void scanFiles() throws IOException, URISyntaxException {
    final Path currentDirectory = Paths.get(this.sourceDirectory);
    Preconditions.checkNotNull(currentDirectory);
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
          new Object[]{directory.toString(), e.toString()});
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

  private void computeCheckSum()
      throws URISyntaxException, IOException, ExecutionException, InterruptedException {
    uploadFilesToRsyncList();
    uploadJar();
    executeMainOnCloudRun(Constants.GENERATE_CHECK_SUM_MAIN);
  }

  private void uploadJar() throws URISyntaxException, IOException {
    String resourceName = Constants.JAR_FILE_NAME;
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

  private void uploadFilesToRsyncList() throws URISyntaxException, IOException {
    ByteSink byteSink =
        gcsStorage.newByteSink(new URI(tmpBucket).resolve(Constants.FILES_TO_RSYNC_FILE_NAME));

    try (BufferedWriter writer =
        new BufferedWriter(new OutputStreamWriter(byteSink.openBufferedStream()))) {
      for (Path file : filesToRsync) {
        writer.write(file.getFileName().toString());
        writer.newLine();
      }
    }
  }

  private void executeMainOnCloudRun(String mainClassPath)
      throws URISyntaxException, IOException, ExecutionException, InterruptedException {
    String downloadJarCommand =
        String.format(
            "gcloud storage cp %s .", new URI(tmpBucket).resolve(Constants.JAR_FILE_NAME));

    String command =
        String.format(
            "java -cp %s "
                + String.format("%s ", mainClassPath)
                + "--project %s "
                + "--tmp_bucket %s "
                + "--target_bucket %s",
            Constants.JAR_FILE_NAME,
            project,
            tmpBucket,
            targetBucket);

    runCloudRunJob(String.format("%s && %s", downloadJarCommand, command), project);
  }

  private void runCloudRunJob(String command, String project)
      throws ExecutionException, InterruptedException {
    Job job =
        Job.newBuilder()
            .setTemplate(
                ExecutionTemplate.newBuilder()
                    .setTemplate(
                        TaskTemplate.newBuilder()
                            .setTimeout(cloudRunTaskTimeout)
                            .addContainers(
                                Container.newBuilder()
                                    .setImage("docker.io/getbamba/google-cloud-sdk-java:latest")
                                    .addCommand("/bin/sh")
                                    .addAllArgs(Arrays.asList("-c", command))
                                    .build())
                            .build())
                    .build())
            .build();

    String jobId = String.format("gcsync-cloudrun-%s", UUID.randomUUID());
    CreateJobRequest createJobRequest =
        CreateJobRequest.newBuilder()
            .setParent(String.format("projects/%s/locations/%s", project, location))
            .setJobId(jobId)
            .setJob(job)
            .build();

    jobsClient.createJobAsync(createJobRequest).get();
    JobName jobName = JobName.of(project, location, jobId);
    OperationFuture<Execution, Execution> future =
        jobsClient
            .runJobOperationCallable()
            .futureCall(RunJobRequest.newBuilder().setName(jobName.toString()).build());

    // Block until the execution of the job completes, this throws if the job fails.
    future.get();

    // Delete the job to prevent it from spamming cloud jobs list
    jobsClient.deleteJobAsync(jobName).get();
  }

  private Path downloadChecksumFile(Path file) throws IOException, URISyntaxException {
    Path tmpCheckSumFile = Util.getTemporaryCheckSumFilePath(file);
    String checksumFileName = Util.getCheckSumFileName(file.getFileName().toString());
    ByteSource byteSource = gcsStorage.newByteSource(new URI(tmpBucket).resolve(checksumFileName));
    byteSource.copyTo(com.google.common.io.Files.asByteSink(tmpCheckSumFile.toFile()));

    return tmpCheckSumFile;
  }

  private void sendRsyncInstructions() throws IOException, URISyntaxException {
    for (Path file : filesToRsync) {
      // Check if we already have an instruction file with a header md5 that matches with the source
      // file's md5. Meaning the file has been changes since we last generated the instruction file.
      String sourceFileMd5 = checkNotNull(fileToMd5.get(file));
      URI instructionFile =
          new URI(tmpBucket).resolve(Util.getInstructionFileName(file.getFileName().toString()));
      Blob blob = gcsStorage.getBlob(instructionFile);
      if (blob != null
          && Util.verifyMd5Header(gcsStorage.newByteSource(instructionFile), sourceFileMd5)) {
        logger.log(
            Level.INFO,
            String.format("Skip generating instructions for file %s which already exists", file));
        continue;
      }

      Path tmpCheckSumFile = downloadChecksumFile(file);
      try (java.io.OutputStream instructionFileOutputStream =
          gcsStorage.newByteSink(instructionFile).openBufferedStream()) {
        try (InputStream inputStream = Files.newInputStream(tmpCheckSumFile)) {
          // The checksum file has an MD5 header that needs to be skipped
          Util.skipMd5Header(inputStream);

          List<com.google.edwmigration.dbsync.proto.Checksum> checksums = getChecksumsFromFile(inputStream);
          ByteSource fileInput = com.google.common.io.Files.asByteSource(file.toFile());

          Util.writeMd5Header(instructionFileOutputStream, sourceFileMd5);
          instructionGenerator.generate(
              instruction -> instruction.writeDelimitedTo(instructionFileOutputStream),
              fileInput,
              checksums);
        }
      } catch (Exception e) {
        if (!gcsStorage.delete(instructionFile)) {
          logger.log(
              Level.SEVERE,
              String.format(
                  "Failed to delete file: %s which is corrupted. Manually delete this file from GCS",
                  instructionFile));
        }
        throw e;
      }

      Files.deleteIfExists(tmpCheckSumFile);
    }
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

  private static List<com.google.edwmigration.dbsync.proto.Checksum> getChecksumsFromFile(InputStream inputStream) throws IOException {
    List<com.google.edwmigration.dbsync.proto.Checksum> checksums = new ArrayList<>();
    com.google.edwmigration.dbsync.proto.Checksum checksum;
    while ((checksum = com.google.edwmigration.dbsync.proto.Checksum.parseDelimitedFrom(inputStream)) != null) {
      checksums.add(checksum);
    }
    inputStream.close();

    return checksums;
  }

  private static String generateMd5(Path file) throws IOException {
    return Base64.getEncoder()
        .encodeToString(
            com.google.common.io.Files.asByteSource(file.toFile()).hash(Hashing.md5()).asBytes());
  }
}