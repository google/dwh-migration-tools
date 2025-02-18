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
import java.util.concurrent.ExecutionException;


public class CloudRunServerAPI {

  // get this from some environment variable
  private static final String BASE_IMAGE = "";
  // get this from some environment variable
  private static final String CLIENT_JAR = "";
  private static final String RSYNC_BINARY_NAME = "rsync-binary";

  private final String project;
  private final String location;
  private final URI stagingBucket;

  private final URI targetUri;

  public CloudRunServerAPI(String project, String location, URI stagingBucket, URI targetUri) {
    this.project = project;
    this.location = location;
    this.stagingBucket = stagingBucket;
    this.targetUri = targetUri;
  }


  private void deployCloudRunJob(String projectId, String location, String jobId,
      String command)
      throws IOException, ExecutionException, InterruptedException {
    try (JobsClient jobsClient = JobsClient.create()) {
      String parent = String.format("projects/%s/locations/%s", projectId, location);
      Job job = Job.newBuilder()
          .setTemplate(
              ExecutionTemplate.newBuilder().setTemplate(TaskTemplate.newBuilder().addContainers(
                  Container.newBuilder()
                      .setImage(BASE_IMAGE)
                      .addCommand("/bin/sh")
                      .addAllArgs(Arrays.asList("-c", command))
                      .build()
              ).build()).build()
          ).build();
      CreateJobRequest jobRequest = CreateJobRequest.newBuilder()
          .setParent(parent)
          .setJobId(jobId)
          .setJob(job)
          .build();
      OperationFuture<Job, Job> client = jobsClient.createJobAsync(jobRequest);
      client.get();
    }
  }

  public void deployRsyncJobs()
      throws IOException, ExecutionException, InterruptedException {
    Path jar_path = Paths.get(CLIENT_JAR);
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
            RSYNC_BINARY_NAME, Mode.GENERATE, project, stagingBucket, targetUri
        );
    deployCloudRunJob(
        project, location, Mode.GENERATE.name().toLowerCase(),
        String.format("%s && %s", jarDownloadCommand, generateCommand)
    );

    // TODO: Handle special characters
    String receiveCommand =
        String.format(
            "java -cp %s "
                + "com.google.edwmigration.dbsync.server.GcsServerMain "
                + "--mode '%s' "
                + "--project '%s' "
                + "--staging_bucket '%s' "
                + "--target_file '%s'",
            RSYNC_BINARY_NAME, Mode.RECEIVE, project, stagingBucket, targetUri
        );
    deployCloudRunJob(
        project, "us-west1", Mode.RECEIVE.name().toLowerCase(),
        String.format("%s && %s", jarDownloadCommand, receiveCommand)
    );
  }

  public void generate() throws IOException, ExecutionException, InterruptedException {
    try (JobsClient jobsClient = JobsClient.create()) {
      jobsClient.runJobAsync(
          JobName.of(project, location,
              String.format("rsync-server-%s", Mode.GENERATE.toString().toLowerCase()))
      ).get();
    }
  }

  public void reconstruct() throws IOException, ExecutionException, InterruptedException {
    try (JobsClient jobsClient = JobsClient.create()) {
      jobsClient.runJobAsync(
          JobName.of(project, location,
              String.format("rsync-server-%s", Mode.RECEIVE.toString().toLowerCase()))
      ).get();
    }
  }
}
