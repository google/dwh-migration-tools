package com.google.edwmigration.dbsync.gcsync;

import static com.google.edwmigration.dbsync.gcsync.Util.getCheckSumFileName;
import static com.google.edwmigration.dbsync.gcsync.Util.getInstructionFileName;
import static com.google.edwmigration.dbsync.gcsync.Util.getListOfFiles;
import static com.google.edwmigration.dbsync.gcsync.Util.getTempFileName;

import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.edwmigration.dbsync.common.DefaultArguments;
import com.google.edwmigration.dbsync.common.InstructionReceiver;
import com.google.edwmigration.dbsync.proto.Instruction;
import com.google.edwmigration.dbsync.storage.gcs.GcsStorage;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import joptsimple.OptionSpec;

public class ReconstructFilesMain {

  private static final Logger logger = Logger.getLogger("gcsync");

  public static void main(String[] args) throws Exception {
    Arguments arguments = new Arguments(args);
    GcsStorage gcsStorage = new GcsStorage(arguments.getOptions().valueOf(
        arguments.projectOptionSpec));
    String tmpBucket = arguments.getOptions().valueOf(arguments.tmpBucketOptionSpec);
    String targetBucket = arguments.getOptions().valueOf(arguments.targetOptionSpec);

    List<String> filesToReconstruct = getListOfFiles(
        gcsStorage.newByteSource(new URI(tmpBucket).resolve(Constants.FILES_TO_RSYNC_FILE_NAME)));

    for (String file : filesToReconstruct) {

      try (InputStream instructionsSource = gcsStorage.newByteSource(
          new URI(tmpBucket).resolve(getInstructionFileName(file))).openBufferedStream()) {
        URI sourceFile = new URI(targetBucket).resolve(file);
        ByteSource baseFileSource = gcsStorage.newByteSource(sourceFile);

        // Create a new file as a temp file and then swap it
        URI tempFile = new URI(targetBucket).resolve(getTempFileName(file));
        ByteSink tmpFileSink = gcsStorage.newByteSink(
            tempFile);

        OutputStream outputStream = tmpFileSink.openBufferedStream();
        try (InstructionReceiver instructionReceiver =
            new InstructionReceiver(outputStream, baseFileSource)) {
          Instruction instruction;
          while ((instruction = Instruction.parseDelimitedFrom(instructionsSource)) != null) {
            instructionReceiver.receive(instruction);
          }
        }

        gcsStorage.copyFile(tempFile, sourceFile);
        gcsStorage.delete(tempFile);
        deleteStagingFiles(gcsStorage, tmpBucket, file);
      }
      gcsStorage.delete(new URI(tmpBucket).resolve(Constants.FILES_TO_RSYNC_FILE_NAME));

      logger.log(Level.INFO, String.format("Finished reconstructing file: %s", file));
    }
  }

  private static void deleteStagingFiles(GcsStorage gcsStorage, String bucket, String file)
      throws URISyntaxException {
    gcsStorage.delete(new URI(bucket).resolve(getInstructionFileName(file)));
    gcsStorage.delete(new URI(bucket).resolve(getCheckSumFileName(file)));
  }

  private static class Arguments extends DefaultArguments {

    private final OptionSpec<String> projectOptionSpec = parser.accepts("project",
        "Specifies the destination project").withRequiredArg().ofType(String.class).required();

    private final OptionSpec<String> targetOptionSpec = parser.accepts("target_bucket",
        "Specifies the target bucket").withRequiredArg().ofType(String.class).required();

    private final OptionSpec<String> tmpBucketOptionSpec = parser.accepts("tmp_bucket",
        "Specifies the temporary bucket").withRequiredArg().ofType(String.class).required();

    public Arguments(String[] args) {
      super(args);
    }
  }
}
