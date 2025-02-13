package com.google.edwmigration.dbsync.client;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.edwmigration.dbsync.common.ChecksumGenerator;
import com.google.edwmigration.dbsync.common.InstructionReceiver;
import com.google.edwmigration.dbsync.proto.Checksum;
import com.google.edwmigration.dbsync.proto.Instruction;
import com.google.edwmigration.dbsync.storage.gcs.GcsStorage;
import com.google.cloud.run.v2.Container;
import com.google.cloud.run.v2.ExecutionTemplate;
import com.google.cloud.run.v2.Job;
import com.google.cloud.run.v2.JobName;
import com.google.cloud.run.v2.TaskTemplate;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import com.google.cloud.run.v2.CreateJobRequest;
import com.google.cloud.run.v2.JobsClient;
import java.util.concurrent.ExecutionException;


public class CloudRunHelper {

  public enum Mode {
    GENERATE, RECEIVE
  }

  private static final int CHECKSUM_BLOCK_SIZE = 4096;
  // get this from some environment variable
  private static final String BASE_IMAGE = "";
  // get this from some environment variable
  private static final String CLIENT_JAR = "";
  private static final String RSYNC_BINARY_NAME = "rsync-binary";

  public static URI getInstructionURI(URI stagingBucket, URI targetUri) throws URISyntaxException {
    return stagingBucket.resolve(targetUri.getPath()).resolve("instruction");
  }

  public static URI getChecksumURI(URI stagingBucket, URI targetUri) throws URISyntaxException {
    return stagingBucket.resolve(targetUri.getPath()).resolve("checksum");
  }

  private static URI getStagedFileURI(URI stagingBucket, URI targetUri) throws URISyntaxException {
    return stagingBucket.resolve(targetUri.getPath()).resolve("staged");
  }

  public static void generate(String projectId, URI stagingBucket, URI targetUri)
      throws IOException, URISyntaxException {
    GcsStorage stagingStorage = new GcsStorage(projectId);
    GcsStorage targetStorage = new GcsStorage(projectId);
    ByteSource source = targetStorage.newByteSource(targetUri);
    ByteSink checksumSink = stagingStorage.newByteSink(
        getChecksumURI(stagingBucket, targetUri)
    );
    ChecksumGenerator generator = new ChecksumGenerator(CHECKSUM_BLOCK_SIZE);
    List<Checksum> checksums = new ArrayList<>();
    generator.generate(checksums::add, source);
    try (OutputStream checksumStream = checksumSink.openBufferedStream()) {
      for (Checksum checksum : checksums) {
        checksum.writeDelimitedTo(checksumStream);
      }
    }
  }

  public static void reconstruct(String projectId, URI stagingBucket, URI targetUri)
      throws IOException, URISyntaxException {
    GcsStorage stagingStorage = new GcsStorage(projectId);
    GcsStorage targetStorage = new GcsStorage(projectId);
    ByteSource instructionSource = stagingStorage.newByteSource(
        getInstructionURI(stagingBucket, targetUri)
    );
    ByteSource baseData = targetStorage.newByteSource(targetUri);

    ByteSink sink = targetStorage.newByteSink(getStagedFileURI(stagingBucket, targetUri));
    try (OutputStream targetStream = sink.openBufferedStream();
        InputStream instructionStream = instructionSource.openBufferedStream()) {
      InstructionReceiver receiver = new InstructionReceiver(targetStream, baseData);
      Instruction instruction;
      while ((instruction = Instruction.parseDelimitedFrom(instructionStream)) != null) {
        receiver.receive(instruction);
      }
    }
  }

  public static void deployCloudRunJob(String projectId, String location, String jobId ,String command)
      throws IOException, ExecutionException, InterruptedException {
    try (JobsClient jobsClient = JobsClient.create()) {
      String parent = String.format("projects/%s/locations/%s", projectId, location);
      Job job = Job.newBuilder()
          .setTemplate(
              ExecutionTemplate.newBuilder().setTemplate(TaskTemplate.newBuilder().addContainers(
                  Container.newBuilder()
                      .setImage(BASE_IMAGE)
                      .addCommand("/bin/sh")
                      .addAllArgs(List.of("-c", command))
                      .build()
              ).build()).build()
          ).build();
      CreateJobRequest jobRequest = CreateJobRequest.newBuilder()
          .setParent(parent)
          .setJobId(jobId)
          .setJob(job)
          .build();
      OperationFuture<Job, Job> client =  jobsClient.createJobAsync(jobRequest);
      client.get();
    }
  }

  public static void deployRsyncJobs(String projectId, URI stagingBucket, URI targetUri)
      throws IOException, ExecutionException, InterruptedException {
    Path jar_path = Paths.get(CLIENT_JAR);
    GcsStorage stagingStorage = new GcsStorage(projectId);
    URI serverJarUri = stagingBucket.resolve(RSYNC_BINARY_NAME);
    stagingStorage.uploadFile(jar_path, serverJarUri);
    String jarDownloadCommand = String.format("gcloud storage cp %s client-all.jar", serverJarUri);

    String generateCommand =
        String.format(
            "java -cp client-all.jar "
                + "com.google.edwmigration.dbsync.client.CloudRunMain "
                + "--mode %s "
                + "--project %s "
                + "--staging_bucket %s "
                + "--target_file %s",
            Mode.GENERATE, projectId, stagingBucket, targetUri
        );
    deployCloudRunJob(
        projectId, "us-west1", Mode.GENERATE.name().toLowerCase(),
        String.format("%s && %s", jarDownloadCommand, generateCommand)
    );

    String receiveCommand =
        String.format(
            "java -cp client-all.jar "
                + "com.google.edwmigration.dbsync.client.CloudRunMain "
                + "--mode %s "
                + "--project %s "
                + "--staging_bucket %s "
                + "--target_file %s",
            Mode.RECEIVE, projectId, stagingBucket, targetUri
        );
    deployCloudRunJob(
        projectId, "us-west1", Mode.RECEIVE.name().toLowerCase(),
        String.format("%s && %s", jarDownloadCommand, receiveCommand)
    );
  }

  public static void runRsyncJob(String projectId,  String location, Mode mode)
      throws IOException, ExecutionException, InterruptedException {
    try (JobsClient jobsClient = JobsClient.create()) {
      jobsClient.runJobAsync(
          JobName.of(projectId, location, String.format("rsync-server-%s", mode.toString().toLowerCase()))
      ).get();
    }
  }
}
