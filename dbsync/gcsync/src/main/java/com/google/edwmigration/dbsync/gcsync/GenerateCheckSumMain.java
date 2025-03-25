package com.google.edwmigration.dbsync.gcsync;

import static com.google.edwmigration.dbsync.gcsync.Util.getListOfFiles;

import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.edwmigration.dbsync.common.ChecksumGenerator;
import com.google.edwmigration.dbsync.common.DefaultArguments;
import com.google.edwmigration.dbsync.storage.gcs.GcsStorage;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import joptsimple.OptionSpec;


public class GenerateCheckSumMain {

  private static final Logger logger = Logger.getLogger("gcsync");

  public static void main(String[] args) throws Exception {
    Arguments arguments = new Arguments(args);
    GcsStorage gcsStorage = new GcsStorage(
        arguments.getOptions().valueOf(arguments.projectOptionSpec));
    String tmpBucket = arguments.getOptions().valueOf(arguments.tmpBucketOptionSpec);
    String targetBucket = arguments.getOptions().valueOf(arguments.targetOptionSpec);

    ChecksumGenerator checksumGenerator = new ChecksumGenerator(Constants.BLOCK_SIZE);
    List<String> filesToGenerateCheckSum = getListOfFiles(
        gcsStorage.newByteSource(new URI(tmpBucket).resolve(Constants.FILES_TO_RSYNC_FILE_NAME)));

    for (String file : filesToGenerateCheckSum) {
      ByteSource byteSource = gcsStorage.newByteSource(new URI(targetBucket).resolve(file));
      if (byteSource.isEmpty()) {
        logger.log(Level.INFO, String.format("File %s has been deleted on target", file));
        continue;
      }

      ByteSink byteSink = gcsStorage.newByteSink(
          new URI(tmpBucket).resolve(Util.getCheckSumFileName(file)));
      try (OutputStream bufferedOutputStream = byteSink.openBufferedStream()) {
        checksumGenerator.generate(checksum ->
            checksum.writeDelimitedTo(bufferedOutputStream), byteSource);
        logger.log(Level.INFO,
            String.format("Finished generating check sum for: %s", file));
      }
    }
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

