package com.google.edwmigration.dbsync.gcsync;

import static com.google.edwmigration.dbsync.gcsync.Util.getListOfFiles;
import static com.google.edwmigration.dbsync.gcsync.Util.verifyMd5Header;

import com.google.cloud.storage.Blob;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.edwmigration.dbsync.common.ChecksumGenerator;
import com.google.edwmigration.dbsync.common.DefaultArguments;
import com.google.edwmigration.dbsync.storage.gcs.GcsStorage;
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
    GcsStorage gcsStorage =
        new GcsStorage(arguments.getOptions().valueOf(arguments.projectOptionSpec));
    String tmpBucket = arguments.getOptions().valueOf(arguments.tmpBucketOptionSpec);
    String targetBucket = arguments.getOptions().valueOf(arguments.targetOptionSpec);
    String filesToRsyncFileName = arguments.getOptions().valueOf(arguments.filesToRsyncFileName);

    ChecksumGenerator checksumGenerator = new ChecksumGenerator(Constants.BLOCK_SIZE);
    List<String> filesToGenerateCheckSum =
        getListOfFiles(gcsStorage.newByteSource(new URI(tmpBucket).resolve(filesToRsyncFileName)));

    for (String file : filesToGenerateCheckSum) {
      URI targetFile = new URI(targetBucket).resolve(file);
      ByteSource byteSource = gcsStorage.newByteSource(targetFile);
      if (byteSource.isEmpty()) {
        logger.log(Level.INFO, String.format("File %s has been deleted on target", file));
        continue;
      }

      String targetFileMd5 = gcsStorage.getBlob(targetFile).getMd5();
      // Check if we already have a checksum file with a header md5 that matches with the target
      // file's md5. Meaning the file has been changes since we generated the checksum file.
      URI checkSumFile = new URI(tmpBucket).resolve(Util.getCheckSumFileName(file));
      Blob blob = gcsStorage.getBlob(checkSumFile);
      if (blob != null && verifyMd5Header(gcsStorage.newByteSource(checkSumFile), targetFileMd5)) {
        logger.log(
            Level.INFO,
            String.format("Skip generating checksum for file %s which already exists", file));
        continue;
      }

      ByteSink byteSink = gcsStorage.newByteSink(checkSumFile);
      try (OutputStream bufferedOutputStream = byteSink.openBufferedStream()) {
        // We write the md5 of the target file as a header of the checksum file
        Util.writeMd5Header(bufferedOutputStream, targetFileMd5);
        checksumGenerator.generate(
            checksum -> checksum.writeDelimitedTo(bufferedOutputStream), byteSource);
        logger.log(Level.INFO, String.format("Finished generating check sum for: %s", file));
      } catch (Exception e) {
        if (!gcsStorage.delete(checkSumFile)) {
          logger.log(
              Level.SEVERE,
              String.format(
                  "Failed to delete file: %s which is corrupted. Manually delete this file from GCS"));
        }
        throw e;
      }
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
