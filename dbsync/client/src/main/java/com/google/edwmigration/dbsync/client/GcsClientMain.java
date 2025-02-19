package com.google.edwmigration.dbsync.client;

import com.google.edwmigration.dbsync.common.DefaultArguments;
import java.net.URI;
import java.util.logging.Logger;
import java.util.logging.Level;
import joptsimple.OptionSpec;

public class GcsClientMain {

  private static class Arguments extends DefaultArguments {

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

    private final OptionSpec<String> sourceOptionSpec =
        parser.accepts("source_file", "Specifies the source file")
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

    public String getProject() {
      return getOptions().valueOf(projectOptionSpec);
    }

    public String getTargetUri() {
      return getOptions().valueOf(targetOptionSpec);
    }

    public String getSourceUri() {return getOptions().valueOf(sourceOptionSpec);}

    public String getStagingBucket() {
      return getOptions().valueOf(stagingBucketOptionSpec);
    }
  }

  public static void main(String[] args) {
    RsyncClient client = new RsyncClient();
    Arguments arguments = new Arguments(args);
    try {
      client.putRsync(
          arguments.getProject(),
          new URI(arguments.getSourceUri()),
          new URI(arguments.getStagingBucket()),
          new URI(arguments.getTargetUri())
      );
    } catch (Exception e){
      Logger.getLogger("rsync").log(Level.INFO, e.getMessage(), e);
    }
  }
}
