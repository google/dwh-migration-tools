package com.google.edwmigration.dbsync.gcsync;

import static com.google.edwmigration.dbsync.gcsync.Util.getCheckSumFileName;
import static com.google.edwmigration.dbsync.gcsync.Util.getInstructionFileName;
import static com.google.edwmigration.dbsync.gcsync.Util.getListOfFiles;
import static com.google.edwmigration.dbsync.gcsync.Util.getTempFileName;
import static com.google.edwmigration.dbsync.gcsync.Util.skipMd5Header;

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
import joptsimple.OptionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReconstructFilesMain {

  private static final Logger logger = LoggerFactory.getLogger(ReconstructFilesMain.class);

  public static void main(String[] args) throws Exception {
    Arguments arguments = new Arguments(args);
    GcsStorage gcsStorage =
        new GcsStorage(arguments.getOptions().valueOf(arguments.projectOptionSpec));
    String tmpBucket = arguments.getOptions().valueOf(arguments.tmpBucketOptionSpec);
    String targetBucket = arguments.getOptions().valueOf(arguments.targetOptionSpec);
    String filesToRsyncFileName = arguments.getOptions().valueOf(arguments.filesToRsyncFileName);

    List<String> filesToReconstruct =
        getListOfFiles(gcsStorage.newByteSource(new URI(tmpBucket).resolve(filesToRsyncFileName)));

    for (String file : filesToReconstruct) {

      try (InputStream instructionsSource =
          gcsStorage
              .newByteSource(new URI(tmpBucket).resolve(getInstructionFileName(file)))
              .openBufferedStream()) {
        URI fileToBeReconstructed = new URI(targetBucket).resolve(file);
        ByteSource baseFileSource = gcsStorage.newByteSource(fileToBeReconstructed);

        // Create a new file as a temp file and then swap it
        URI tmpFile = new URI(targetBucket).resolve(getTempFileName(file));
        ByteSink tmpFileSink = gcsStorage.newByteSink(tmpFile);

        OutputStream outputStream = tmpFileSink.openBufferedStream();

        // The instruction file has a md5 header of the source file being synced from.
        String sourceFileMd5 = skipMd5Header(instructionsSource);
        try (InstructionReceiver instructionReceiver =
            new InstructionReceiver(outputStream, baseFileSource)) {
          Instruction instruction;
          while ((instruction = Instruction.parseDelimitedFrom(instructionsSource)) != null) {
            instructionReceiver.receive(instruction);
          }
        }

        verifyMd5(sourceFileMd5, gcsStorage, tmpFile, fileToBeReconstructed);

        // Clean up
        gcsStorage.delete(tmpFile);
        deleteStagingFiles(gcsStorage, tmpBucket, file);
      }
      gcsStorage.delete(new URI(tmpBucket).resolve(filesToRsyncFileName));

      logger.info("Finished reconstructing file: %s", file);
    }
  }

  private static void verifyMd5(
      String sourceFileMd5, GcsStorage gcsStorage, URI tmpFile, URI fileToBeReconstructed) {
    if (sourceFileMd5.equals(gcsStorage.getBlob(tmpFile).getMd5())) {
      gcsStorage.copyFile(tmpFile, fileToBeReconstructed);

    } else {
      logger.info(
          String.format(
              "The reconstructed file of %s doesn't match the file on the source file, the file might be"
                  + " corrupted or the source file might have been changed while the tool is running",
              fileToBeReconstructed));
    }
  }

  private static void deleteStagingFiles(GcsStorage gcsStorage, String bucket, String file)
      throws URISyntaxException {
    gcsStorage.delete(new URI(bucket).resolve(getInstructionFileName(file)));
    gcsStorage.delete(new URI(bucket).resolve(getCheckSumFileName(file)));
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

    private final OptionSpec<String> filesToRsyncFileName =
        parser
            .accepts("file_name", "The name of the file containing the list of files to be rsynced")
            .withRequiredArg()
            .ofType(String.class)
            .required();

    public Arguments(String[] args) {
      super(args);
    }
  }
}
