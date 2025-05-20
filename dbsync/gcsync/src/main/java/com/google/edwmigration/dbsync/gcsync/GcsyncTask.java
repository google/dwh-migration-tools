package com.google.edwmigration.dbsync.gcsync;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.edwmigration.dbsync.gcsync.Util.verifyMd5Header;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.run.v2.Container;
import com.google.cloud.run.v2.CreateJobRequest;
import com.google.cloud.run.v2.Execution;
import com.google.cloud.run.v2.ExecutionTemplate;
import com.google.cloud.run.v2.Job;
import com.google.cloud.run.v2.JobName;
import com.google.cloud.run.v2.JobsClient;
import com.google.cloud.run.v2.ResourceRequirements;
import com.google.cloud.run.v2.RunJobRequest;
import com.google.cloud.run.v2.TaskTemplate;
import com.google.cloud.storage.Blob;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.edwmigration.dbsync.common.InstructionGenerator;
import com.google.edwmigration.dbsync.proto.Checksum;
import com.google.edwmigration.dbsync.storage.gcs.GcsStorage;
import com.google.protobuf.Duration;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsyncTask implements Callable<Void> {

  private final List<Path> filesToRsync;
  private final String targetBucket;

  private final String tmpBucket;

  private final String project;

  private final String location;

  private final Duration cloudRunTaskTimeout;

  private static final Logger logger = LoggerFactory.getLogger(GcsyncTask.class);

  private final GcsStorage gcsStorage;

  private final JobsClient jobsClient;

  private final InstructionGenerator instructionGenerator;

  private final Map<Path, String> fileToMd5;

  private final String filesToRsyncFileName;

  /** A task that executes cloud run task to rsync the files and then upload the files that */
  public GcsyncTask(
      List<Path> filesToRsync,
      String targetBucket,
      String tmpBucket,
      String project,
      String location,
      Duration cloudRunTaskTimeout,
      GcsStorage gcsStorage,
      JobsClient jobsClient,
      InstructionGenerator instructionGenerator,
      Map<Path, String> fileToMd5) {
    this.filesToRsync = filesToRsync;
    this.targetBucket = targetBucket;
    this.tmpBucket = tmpBucket;
    this.project = project;
    this.location = location;
    this.cloudRunTaskTimeout = cloudRunTaskTimeout;
    this.gcsStorage = gcsStorage;
    this.jobsClient = jobsClient;
    this.instructionGenerator = instructionGenerator;
    this.fileToMd5 = fileToMd5;

    filesToRsyncFileName = UUID.randomUUID() + "_" + Constants.FILES_TO_RSYNC_FILE_NAME;
  }

  @Override
  public Void call() throws Exception {
    if (!filesToRsync.isEmpty()) {
      // Upload the list of files to be rsynced handled by this task to gcs tmp bucket.
      uploadFilesToRsyncList();
      // Compute checksum, executed on cloud run.
      computeCheckSum();
      // Download the checksum file from GCS. Compute & send instruction files to gcs.
      sendRsyncInstructions();
      // Reconstruct files on gcs using instructions.
      reconStructFiles();
      // Upload files that cannot be rsynced and small files.
    }

    return null;
  }

  private void computeCheckSum()
      throws URISyntaxException, ExecutionException, InterruptedException {
    executeMainOnCloudRun(Constants.GENERATE_CHECK_SUM_MAIN);
  }

  private void uploadFilesToRsyncList() throws URISyntaxException, IOException {
    ByteSink byteSink = gcsStorage.newByteSink(new URI(tmpBucket).resolve(filesToRsyncFileName));

    try (BufferedWriter writer =
        new BufferedWriter(new OutputStreamWriter(byteSink.openBufferedStream()))) {
      for (Path file : filesToRsync) {
        writer.write(file.getFileName().toString());
        writer.newLine();
      }
    }
  }

  private void executeMainOnCloudRun(String mainClassPath)
      throws URISyntaxException, ExecutionException, InterruptedException {
    String downloadJarCommand =
        String.format(
            "gcloud storage cp %s .", new URI(tmpBucket).resolve(Constants.JAR_FILE_NAME));

    String command =
        String.format(
            "java -cp %s "
                + String.format("%s ", mainClassPath)
                + "--project %s "
                + "--tmp_bucket %s "
                + "--target_bucket %s "
                + "--file_name %s",
            Constants.JAR_FILE_NAME,
            project,
            tmpBucket,
            targetBucket,
            filesToRsyncFileName);

    runCloudRunJob(String.format("%s && %s", downloadJarCommand, command), project);
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
          && verifyMd5Header(gcsStorage.newByteSource(instructionFile), sourceFileMd5)) {
        logger.info(
            String.format("Skip generating instructions for file %s which already exists", file));
        continue;
      }

      Path tmpCheckSumFile = downloadChecksumFile(file);
      try (OutputStream instructionFileOutputStream =
          gcsStorage.newByteSink(instructionFile).openBufferedStream()) {
        try (InputStream inputStream = Files.newInputStream(tmpCheckSumFile)) {
          // The checksum file has an MD5 header that needs to be skipped
          Util.skipMd5Header(inputStream);

          List<Checksum> checksums = getChecksumsFromFile(inputStream);
          ByteSource fileInput = com.google.common.io.Files.asByteSource(file.toFile());

          Util.writeMd5Header(instructionFileOutputStream, sourceFileMd5);
          instructionGenerator.generate(
              instruction -> instruction.writeDelimitedTo(instructionFileOutputStream),
              fileInput,
              checksums);
        }
      } catch (Exception e) {
        if (!gcsStorage.delete(instructionFile)) {
          logger.info(
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
      throws ExecutionException, InterruptedException, URISyntaxException {
    logger.info("reconstruct");
    executeMainOnCloudRun(Constants.RECONSTRUCT_FILE_MAIN);
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
                                    .setResources(
                                        ResourceRequirements.newBuilder()
                                            .putLimits("memory", "4Gi")
                                            .putLimits("cpu", "2")
                                            .build())
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

  private static List<Checksum> getChecksumsFromFile(InputStream inputStream) throws IOException {
    List<Checksum> checksums = new ArrayList<>();
    Checksum checksum;
    while ((checksum = Checksum.parseDelimitedFrom(inputStream)) != null) {
      checksums.add(checksum);
    }

    return checksums;
  }
}
