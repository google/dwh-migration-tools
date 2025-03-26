package com.google.edwmigration.dbsync.gcsync;

import com.google.api.gax.longrunning.OperationTimedPollAlgorithm;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.run.v2.JobsClient;
import com.google.cloud.run.v2.JobsSettings;
import com.google.edwmigration.dbsync.common.DefaultArguments;
import com.google.edwmigration.dbsync.common.InstructionGenerator;
import com.google.edwmigration.dbsync.storage.gcs.GcsStorage;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import java.io.IOException;
import java.text.ParseException;
import java.util.logging.Level;
import java.util.logging.Logger;
import joptsimple.OptionSpec;


public class GcsyncMain {

  public static void main(String[] args) throws IOException, ParseException {
    Arguments arguments = new Arguments(args);
    String project = arguments.getOptions().valueOf(arguments.projectOptionSpec);
    Duration cloudRunTaskTimeout = Durations.parse(
        arguments.getOptions().valueOf(arguments.cloudRunTaskTimeoutSpec));

    GcsyncClient gcsyncClient = new GcsyncClient(
        project,
        Util.ensureTrailingSlash(arguments.getOptions().valueOf(arguments.tmpBucketOptionSpec)),
        Util.ensureTrailingSlash(arguments.getOptions().valueOf(arguments.targetOptionSpec)),
        arguments.getOptions().valueOf(arguments.locationOptionSpec),
        arguments.getOptions().valueOf(arguments.sourceDirectoryOptionSpec),
        cloudRunTaskTimeout,
        JobsClient.create(jobsSettings(cloudRunTaskTimeout)),
        new GcsStorage(project),
        new InstructionGenerator(Constants.BLOCK_SIZE));
    try {
      gcsyncClient.syncFiles();
    } catch (Exception e) {
      Logger.getLogger("gcsync").log(Level.INFO, e.getMessage(), e);
    }
  }

  private static JobsSettings jobsSettings(Duration jobTimeout) throws IOException {
    JobsSettings.Builder builder = JobsSettings.newBuilder();
    builder.runJobOperationSettings().setPollingAlgorithm(
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

  private static class Arguments extends DefaultArguments {

    private final OptionSpec<String> projectOptionSpec = parser.accepts("project",
        "Specifies the destination project").withRequiredArg().ofType(String.class).required();

    private final OptionSpec<String> targetOptionSpec = parser.accepts("target_bucket",
        "Specifies the target bucket").withRequiredArg().ofType(String.class).required();

    private final OptionSpec<String> tmpBucketOptionSpec = parser.accepts("tmp_bucket",
        "Specifies the temporary bucket").withRequiredArg().ofType(String.class).required();

    private final OptionSpec<String> locationOptionSpec = parser.accepts("location",
            "Specifies the GCP location to run the cloud run jobs").withRequiredArg()
        .ofType(String.class).required();

    private final OptionSpec<String> sourceDirectoryOptionSpec = parser.accepts("source_directory",
            "Specified the source directory to sync files from").withOptionalArg().ofType(String.class)
        .defaultsTo(".");

    private final OptionSpec<String> cloudRunTaskTimeoutSpec = parser.accepts("task_timeout",
            "Specified the time out of the cloud run tasks").withOptionalArg().ofType(String.class)
        .defaultsTo(Constants.CLOUD_RUN_TIMEOUT);

    public Arguments(String[] args) {
      super(args);
    }
  }
}
