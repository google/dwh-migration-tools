package com.google.edwmigration.dbsync.client;

import com.google.edwmigration.dbsync.client.CloudRunHelper.Mode;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import joptsimple.OptionSpec;

public class CloudRunMain {

  private static class Arguments extends DefaultArguments {

    private final OptionSpec<Mode> modeOptionSpec =
        parser.accepts("mode", "Specifies the mode")
            .withRequiredArg()
            .ofType(Mode.class)
            .required();

    private final OptionSpec<String> projectOptionSpec =
        parser.accepts("project", "Specifies the destination project")
            .withRequiredArg()
            .ofType(String.class)
            .required();

    private final OptionSpec<String> targetOptionSpec =
        parser.accepts("target_file", "Specifies the target file")
            .withRequiredArg()
            .ofType(String.class)
            .required();

    private final OptionSpec<String> stagingBucketOptionSpec =
        parser.accepts("staging_bucket", "Specifies the staging bucket")
            .withRequiredArg()
            .ofType(String.class)
            .required();

    public Arguments(String[] args) {
      super(args);
    }

    public Mode getMode() {
      return getOptions().valueOf(modeOptionSpec);
    }

    public String getProject() {
      return getOptions().valueOf(projectOptionSpec);
    }

    public String getTargetUri() {
      return getOptions().valueOf(targetOptionSpec);
    }

    public String getStagingBucket() {
      return getOptions().valueOf(stagingBucketOptionSpec);
    }
  }


  // This is invoked in CloudRun, as ServerMain --mode GENERATE vs --mode RECONSTRUCT
  public static void main(String[] args) throws IOException, URISyntaxException {
    Arguments argument = new Arguments(args);
    switch (argument.getMode()) {
      case GENERATE:
        CloudRunHelper.generate(
          argument.getProject(),
          URI.create(argument.getStagingBucket()),
          URI.create(argument.getTargetUri())
      );
      case RECEIVE:
        CloudRunHelper.reconstruct(
          argument.getProject(),
          URI.create(argument.getStagingBucket()),
          URI.create(argument.getTargetUri())
      );
    }
  }

}
