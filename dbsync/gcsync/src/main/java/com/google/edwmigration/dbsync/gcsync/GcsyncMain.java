package com.google.edwmigration.dbsync.gcsync;

import com.google.common.base.Preconditions;
import com.google.edwmigration.dbsync.common.DefaultArguments;
import com.google.edwmigration.dbsync.storage.gcs.GcsStorage;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import java.text.ParseException;
import java.util.logging.Level;
import java.util.logging.Logger;
import joptsimple.OptionSpec;

public class GcsyncMain {

  public static void main(String[] args) throws ParseException {
    Arguments arguments = new Arguments(args);
    String project = arguments.getOptions().valueOf(arguments.projectOptionSpec);
    Duration cloudRunTaskTimeout =
        Durations.parse(arguments.getOptions().valueOf(arguments.cloudRunTaskTimeoutSpec));
    int numConCurrentTask = arguments.getOptions().valueOf(arguments.numConcurrentTaskSpec);
    Preconditions.checkArgument(numConCurrentTask > 0,
        "--num_concurrent_tasks must be a positive integer");

    GcsyncClient gcsyncClient =
        new GcsyncClient(
            project,
            Util.ensureTrailingSlash(arguments.getOptions().valueOf(arguments.tmpBucketOptionSpec)),
            Util.ensureTrailingSlash(arguments.getOptions().valueOf(arguments.targetOptionSpec)),
            arguments.getOptions().valueOf(arguments.locationOptionSpec),
            arguments.getOptions().valueOf(arguments.sourceDirectoryOptionSpec),
            numConCurrentTask,
            cloudRunTaskTimeout,
            new GcsStorage(project));
    try {
      gcsyncClient.syncFiles();
    } catch (Exception e) {
      Logger.getLogger("gcsync").log(Level.INFO, e.getMessage(), e);
    }
  }


  private static class Arguments extends DefaultArguments {

    private final OptionSpec<String> projectOptionSpec =
        parser
            .accepts("project", "Specifies the destination project")
            .withRequiredArg()
            .ofType(String.class)
            .required();

    private final OptionSpec<String> targetOptionSpec =
        parser
            .accepts("target_bucket", "Specifies the target bucket")
            .withRequiredArg()
            .ofType(String.class)
            .required();

    private final OptionSpec<String> tmpBucketOptionSpec =
        parser
            .accepts("tmp_bucket", "Specifies the temporary bucket")
            .withRequiredArg()
            .ofType(String.class)
            .required();

    private final OptionSpec<String> locationOptionSpec =
        parser
            .accepts("location", "Specifies the GCP location to run the cloud run jobs")
            .withRequiredArg()
            .ofType(String.class)
            .required();

    private final OptionSpec<String> sourceDirectoryOptionSpec =
        parser
            .accepts("source_directory", "Specified the source directory to sync files from")
            .withOptionalArg()
            .ofType(String.class)
            .defaultsTo(".");

    private final OptionSpec<String> cloudRunTaskTimeoutSpec =
        parser
            .accepts("task_timeout", "Specified the time out of the cloud run tasks")
            .withOptionalArg()
            .ofType(String.class)
            .defaultsTo(Constants.CLOUD_RUN_TIMEOUT);

    private final OptionSpec<Integer> numConcurrentTaskSpec =
        parser
            .accepts("num_concurrent_tasks", "Specified the time out of the cloud run tasks")
            .withOptionalArg()
            .ofType(Integer.class)
            .defaultsTo(1);

    public Arguments(String[] args) {
      super(args);
    }
  }
}
