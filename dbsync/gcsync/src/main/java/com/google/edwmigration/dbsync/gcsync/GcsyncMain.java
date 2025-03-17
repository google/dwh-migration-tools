package com.google.edwmigration.dbsync.gcsync;

import com.google.cloud.run.v2.JobsClient;
import com.google.edwmigration.dbsync.common.DefaultArguments;
import com.google.edwmigration.dbsync.common.InstructionGenerator;
import com.google.edwmigration.dbsync.storage.gcs.GcsStorage;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import joptsimple.OptionSpec;


public class GcsyncMain {

  // --source_directory /usr/local/google/home/tonyxiaowei/tony-work
  public static void main(String[] args) throws IOException {
    Arguments arguments = new Arguments(args);
    String project = arguments.getOptions().valueOf(arguments.projectOptionSpec);
    GcsyncClient gcsyncClient = new GcsyncClient(
        project,
        arguments.getOptions().valueOf(arguments.tmpBucketOptionSpec),
        arguments.getOptions().valueOf(arguments.targetOptionSpec),
        arguments.getOptions().valueOf(arguments.locationOptionSpec),
        arguments.getOptions().valueOf(arguments.sourceDirectoryOptionSpec),
        JobsClient.create(),
        new GcsStorage(project),
        new InstructionGenerator(Constants.BLOCK_SIZE));
    try {
      gcsyncClient.syncFiles();
    } catch (Exception e) {
      Logger.getLogger("gcsync").log(Level.INFO, e.getMessage(), e);
    }
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

    public Arguments(String[] args) {
      super(args);
    }
  }
}
