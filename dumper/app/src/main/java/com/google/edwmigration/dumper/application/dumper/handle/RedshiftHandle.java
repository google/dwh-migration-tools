/*
 * Copyright 2022-2025 Google LLC
 * Copyright 2013-2021 CompilerWorks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.edwmigration.dumper.application.dumper.handle;

import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.redshift.AmazonRedshift;
import com.amazonaws.services.redshift.AmazonRedshiftClient;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is thread-safe as log as the provided {@link DataSource} is thread-safe.
 *
 * @author kakha
 */
public class RedshiftHandle extends JdbcHandle {

  private static RedshiftHandle INSTANCE;

  private Optional<AmazonRedshift> redshiftClient;
  private Optional<AmazonCloudWatch> cloudWatchClient;

  /**
   * Gets the single instance of the RedshiftHandle.
   *
   * <p>This method is thread-safe. The arguments are only used during the very first call to create
   * the instance. All subsequent calls will ignore the arguments and return the already-created
   * instance.
   *
   * @param dataSource The JDBC DataSource to wrap.
   * @param arguments The connector arguments for configuration.
   * @return The singleton RedshiftHandle instance.
   * @throws SQLException if there is an issue with the database connection during initialization.
   */
  public static RedshiftHandle getInstance(
      @Nonnull DataSource dataSource, ConnectorArguments arguments) throws SQLException {
    RedshiftHandle result = INSTANCE;
    if (result == null) {
      // Synchronize on the class object to ensure only one thread can create the instance.
      synchronized (RedshiftHandle.class) {
        result = INSTANCE; // Re-check inside the synchronized block.
        if (result == null) {
          INSTANCE = result = new RedshiftHandle(dataSource, arguments);
        }
      }
    }
    return result;
  }

  /**
   * The constructor is private to make class singleton.
   *
   * @param dataSource The JDBC DataSource to wrap.
   * @param arguments The connector arguments for configuration.
   * @throws SQLException if there is an issue with the database connection.
   */
  private RedshiftHandle(@Nonnull DataSource dataSource, ConnectorArguments arguments)
      throws SQLException {
    super(
        new HikariDataSource(
            HandleUtil.createHikariConfig(dataSource, arguments.getThreadPoolSize())));

    // AWS API tasks, enabled by default if IAM credentials are provided
    Optional<AWSCredentialsProvider> awsCredentials = createCredentialsProvider(arguments);
    redshiftClient = Optional.empty();
    this.cloudWatchClient = Optional.empty();

    awsCredentials.ifPresent(
        awsCreds -> {
          this.redshiftClient =
              Optional.ofNullable(AmazonRedshiftClient.builder().withCredentials(awsCreds).build());
          this.cloudWatchClient =
              Optional.ofNullable(
                  AmazonCloudWatchClient.builder().withCredentials(awsCreds).build());
        });
  }

  public Optional<AmazonRedshift> getRedshiftClient() {
    return redshiftClient;
  }

  public Optional<AmazonCloudWatch> getCloudWatchClient() {
    return cloudWatchClient;
  }

  @Override
  public void close() throws IOException {
    DataSource ds = getDataSource();
    if (ds instanceof AutoCloseable) {
      try {
        ((AutoCloseable) ds).close();
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException("Failed to close DataSource: " + e, e);
      }
    }

    redshiftClient.ifPresent(AmazonRedshift::shutdown);
    cloudWatchClient.ifPresent(AmazonCloudWatch::shutdown);
  }

  private Optional<AWSCredentialsProvider> createCredentialsProvider(ConnectorArguments arguments) {
    String profileName = arguments.getIAMProfile();
    if (profileName != null) {
      return Optional.of(new ProfileCredentialsProvider(profileName));
    }
    String accessKeyId = arguments.getIAMAccessKeyID();
    String secretAccessKey = arguments.getIAMSecretAccessKey();
    String sessionToken = arguments.getIamSessionToken();
    if (accessKeyId == null || secretAccessKey == null) {
      return Optional.empty();
    }

    if (sessionToken != null) {
      BasicSessionCredentials credentials =
          new BasicSessionCredentials(accessKeyId, secretAccessKey, sessionToken);
      return Optional.of(new AWSStaticCredentialsProvider(credentials));
    }
    BasicAWSCredentials credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
    return Optional.of(new AWSStaticCredentialsProvider(credentials));
  }
}
