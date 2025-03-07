package com.google.edwmigration.dbsync.server;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.edwmigration.dbsync.server.GcsServerMain.Mode;
import com.google.edwmigration.dbsync.storage.gcs.GcsStorage;
import com.google.cloud.run.v2.Container;
import com.google.cloud.run.v2.ExecutionTemplate;
import com.google.cloud.run.v2.Job;
import com.google.cloud.run.v2.JobName;
import com.google.cloud.run.v2.TaskTemplate;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import com.google.cloud.run.v2.CreateJobRequest;
import com.google.cloud.run.v2.JobsClient;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class CloudRunServerAPI {
  private static final String BASE_IMAGE_ENV_VAR = "RSYNC_CLOUD_BASE_IMAGE";
  private static final String SERVER_JAR_ENV_VAR = "RSYNC_SERVER_JAR";
  private static final String RSYNC_BINARY_NAME = "rsync-binary";

  private final String project;
  private final String location;
  private final URI stagingBucket;
  private final URI targetUri;
  private final String baseImage;
  private final String clientJar;
  private final String jobIdSuffix;
  private final boolean deleteStagingFiles;

  public CloudRunServerAPI(
      String project,
      String location,
      URI stagingBucket,
      URI targetUri,
      boolean deleteStagingFiles) {
    this.project = project;
    this.location = location;
    this.stagingBucket = stagingBucket;
    this.targetUri = targetUri;
    this.baseImage = System.getenv(BASE_IMAGE_ENV_VAR);
    this.clientJar = System.getenv(SERVER_JAR_ENV_VAR);
    // TODO a better way to generate unique id for job.
    this.jobIdSuffix = UUID.randomUUID().toString();
    this.deleteStagingFiles = deleteStagingFiles;
  }

  private String getJobId(Mode mode) {
    return String.format("rsync-server-%s-%s", mode.toString().toLowerCase(), this.jobIdSuffix);
  }

  private void deployCloudRunJob(String projectId, String location, Mode mode, String command)
      throws IOException, ExecutionException, InterruptedException {
    try (JobsClient jobsClient = JobsClient.create()) {
      String parent = String.format("projects/%s/locations/%s", projectId, location);
      Job job =
          Job.newBuilder()
              .setTemplate(
                  ExecutionTemplate.newBuilder()
                      .setTemplate(
                          TaskTemplate.newBuilder()
                              .addContainers(
                                  Container.newBuilder()
                                      .setImage(this.baseImage)
                                      .addCommand("/bin/sh")
                                      .addAllArgs(Arrays.asList("-c", command))
                                      .build())
                              .build())
                      .build())
              .build();
      CreateJobRequest jobRequest =
          CreateJobRequest.newBuilder()
              .setParent(parent)
              .setJobId(getJobId(mode))
              .setJob(job)
              .build();
      OperationFuture<Job, Job> client = jobsClient.createJobAsync(jobRequest);
      client.get();
    }
  }

  public void deployRsyncJobs() throws IOException, ExecutionException, InterruptedException {
    Path jar_path = Paths.get(this.clientJar);
    GcsStorage stagingStorage = new GcsStorage(project);
    URI serverJarUri = stagingBucket.resolve(RSYNC_BINARY_NAME);
    stagingStorage.uploadFile(jar_path, serverJarUri);
    String jarDownloadCommand = String.format("gcloud storage cp %s .", serverJarUri);

    // TODO: Handle special characters
    String generateCommand =
        String.format(
            "java -cp %s "
                + "com.google.edwmigration.dbsync.server.GcsServerMain "
                + "--mode '%s' "
                + "--project '%s' "
                + "--staging_bucket '%s' "
                + "--target_file '%s'",
            RSYNC_BINARY_NAME, Mode.GENERATE, project, stagingBucket, targetUri);
    deployCloudRunJob(
        project,
        location,
        Mode.GENERATE,
        String.format("%s && %s", jarDownloadCommand, generateCommand));

    // TODO: Handle special characters
    String receiveCommand =
        String.format(
            "java -cp %s "
                + "com.google.edwmigration.dbsync.server.GcsServerMain "
                + "--mode '%s' "
                + "--project '%s' "
                + "--staging_bucket '%s' "
                + "--target_file '%s' "
                + "--delete_staging_files %s ",
            RSYNC_BINARY_NAME, Mode.RECEIVE, project, stagingBucket, targetUri, this.deleteStagingFiles);
    deployCloudRunJob(
        project,
        location,
        Mode.RECEIVE,
        String.format("%s && %s", jarDownloadCommand, receiveCommand));
  }

  public void generate() throws IOException, ExecutionException, InterruptedException {
    try (JobsClient jobsClient = JobsClient.create()) {
      jobsClient.runJobAsync(JobName.of(project, location, getJobId(Mode.GENERATE))).get();
    }
  }

  public void reconstruct() throws IOException, ExecutionException, InterruptedException {
    try (JobsClient jobsClient = JobsClient.create()) {
      jobsClient.runJobAsync(JobName.of(project, location, getJobId(Mode.RECEIVE))).get();
    }
  }
}
