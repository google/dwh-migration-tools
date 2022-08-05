/*
 * Copyright 2022 Google LLC
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
package com.google.edwmigration.dumper.application.dumper;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.io.FileSystemOutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.task.ArgumentsTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcRunSQLScript;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetState;
import com.google.edwmigration.dumper.application.dumper.task.TaskState;
import com.google.edwmigration.dumper.application.dumper.task.VersionTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandle;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskGroup;
import com.google.edwmigration.dumper.application.dumper.task.TaskResult;

/**
 *
 *
 * @author miguel
 */
public class MetadataDumper {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataDumper.class);

    private static final ImmutableMap<String, Connector> CONNECTORS;

    static {
        ImmutableMap.Builder<String, Connector> builder = ImmutableMap.builder();
        for (Connector connector : ServiceLoader.load(Connector.class))
            builder.put(connector.getName().toUpperCase(), connector);
        CONNECTORS = builder.build();
    }

    private boolean exitOnError = true;
    public MetadataDumper withExitOnError(boolean state) {
        exitOnError = state;
        return this;
    }

    @Nonnull
    private static String repeat(@Nonnull char c, @Nonnegative int n) {
        char[] out = new char[n];
        Arrays.fill(out, c);
        return new String(out);
    }

    public void run(@Nonnull String... args) throws Exception {
        ConnectorArguments arguments = new ConnectorArguments(args);

        String connectorName = arguments.getConnectorName();
        if (connectorName == null) {
            LOG.error("Target DBMS is required");
            return;
        }

        Connector connector = CONNECTORS.get(connectorName.toUpperCase());
        if (connector == null) {
            LOG.error("Target DBMS " + connectorName + " not supported; available are " + CONNECTORS.keySet() + ".");
            return;
        }

        try {
            run(connector, arguments);
        } finally {
            if (arguments.saveResponseFile())
                JsonResponseFile.save(arguments);
        }
    }

    @CheckForNull
    private <T> T runTask(@Nonnull TaskRunContext context, @Nonnull TaskSetState.Impl state, Task<T> task) throws MetadataDumperUsageException {
        try {
            CHECK:
            {
                TaskState ts = state.getTaskState(task);
                Preconditions.checkState(ts == TaskState.NOT_STARTED, "TaskState was bad: " + ts + " for " + task);
            }

            PRECONDITION:
            for (Task.Condition condition : task.getConditions()) {
                if (!condition.evaluate(state)) {
                    LOG.debug("Skipped " + task.getName() + " because " + condition.toSkipReason());
                    state.setTaskResult(task, TaskState.SKIPPED, null);
                    return null;
                }
            }

            RUN:
            {
                T value = task.run(context);
                state.setTaskResult(task, TaskState.SUCCEEDED, value);
                return value;
            }
        } catch (Exception e) {
            // MetadataDumperUsageException should be fatal.
            if (e instanceof MetadataDumperUsageException)
                throw (MetadataDumperUsageException) e;
            if (e instanceof SQLException && e.getCause() instanceof MetadataDumperUsageException)
                throw (MetadataDumperUsageException) e.getCause();
            // TaskGroup is an attempt to get rid of this condition.
            // We might need an additional TaskRunner / TaskSupport with an overrideable handleException method instead of this runTask() method.
            if (!task.handleException(e))
                LOG.warn("Task failed: " + task + ": " + e, e);
            state.setTaskException(task, TaskState.FAILED, e);
            try {
                OutputHandle sink = context.newOutputFileHandle(task.getTargetPath() + ".exception.txt");
                sink.asCharSink(StandardCharsets.UTF_8).writeLines(Arrays.asList(task.toString(),
                        "******************************",
                        String.valueOf(new DumperDiagnosticQuery(e).call())));
            } catch (Exception f) {
                LOG.warn("Exception-recorder failed:  " + f, f);
            }
        }
        return null;
    }

    @EnsuresNonNullIf(expression = "#1", result = false)
    private static boolean isNullOrEmpty(@CheckForNull Object[] in) {
        if (in == null)
            return true;
        return in.length == 0;
    }

    private void print(@Nonnull Task<?> task, int indent) {
        System.out.println(StringUtils.repeat("  ", indent) + task);
        if (task instanceof TaskGroup) {
            for (Task<?> subtask : ((TaskGroup) task).getTasks())
                print(subtask, indent + 1);
        }
    }

    protected void run(@Nonnull Connector connector, @Nonnull ConnectorArguments arguments) throws Exception {
        List<Task<?>> tasks = new ArrayList<>();
        tasks.add(new VersionTask());
        tasks.add(new ArgumentsTask(arguments));
        {
            File sqlScript = arguments.getSqlScript();
            if (sqlScript != null)
                tasks.add(new JdbcRunSQLScript(sqlScript));
        }
        connector.addTasksTo(tasks, arguments);

        // The default output file is based on the connector.
        // We had a customer request to base it on the database, but that isn't well-defined,
        // as there may be 0 or N databases in a single file.
        File outputFile = arguments.getOutputFile();
        if (outputFile == null)
            outputFile = new File(connector.getDefaultFileName());
        if (arguments.isDryRun()) {
            String title = "Dry run: Printing task list for " + connector.getName();
            System.out.println(title);
            System.out.println(repeat('=', title.length()));
            System.out.println("Writing to " + outputFile);
            for (Task<?> task : tasks) {
                print(task, 1);
            }
        } else {
            Stopwatch stopwatch = Stopwatch.createStarted();
            TaskSetState.Impl state = new TaskSetState.Impl();

            LOG.info("Using " + connector);
            try (Closer closer = Closer.create()) {

                final OutputHandleFactory sinkFactory;
                if (StringUtils.endsWithIgnoreCase(outputFile.getName(), ".zip")) {
                    if (outputFile.exists()) {
                        // If it exists, it must be a file.
                        if (!outputFile.isFile())
                            throw new IllegalStateException("Cannot overwrite existing non-file with file.");
                        if (!arguments.isOutputContinue())
                            outputFile.delete();    // It's a simple file, and we were asked to overwrite it.
                    } else {
                        Files.createParentDirs(outputFile);
                    }

                    URI outputUri = URI.create("jar:" + outputFile.toURI());
                    // LOG.debug("Is a zip file: " + outputUri);
                    Map<String, Object> fileSystemProperties = ImmutableMap.<String, Object>builder()
                            .put("create", "true")
                            .put("useTempFile", Boolean.TRUE)
                            .build();
                    FileSystem fileSystem = closer.register(FileSystems.newFileSystem(outputUri, fileSystemProperties));
                    sinkFactory = new FileSystemOutputHandleFactory(fileSystem, "/");   // It's required to be "/"
                } else {
                    if (outputFile.exists()) {
                        // If it exists, it must be an empty directory, OR we must have specified --continue. We never delete.
                        if (!outputFile.isDirectory())
                            throw new IllegalStateException("Cannot overwrite existing non-directory with directory.");
                        if (!arguments.isOutputContinue() && !isNullOrEmpty(outputFile.list()))
                            throw new IllegalStateException("Refusing to touch non-empty directory without --continue.");
                    } else {
                        // LOG.debug("Not a zip file: " + outputFile);
                        outputFile.mkdirs();
                        if (!outputFile.isDirectory())
                            throw new IOException("Unable to create directory " + outputFile);
                    }

                    sinkFactory = new FileSystemOutputHandleFactory(outputFile.toPath());
                }
                LOG.debug("Target filesystem is " + sinkFactory);

                Handle handle = closer.register(connector.open(arguments));

                TaskRunContext runContext = new TaskRunContext(sinkFactory, handle, arguments.getThreadPoolSize()) {
                    @Override
                    public TaskState getTaskState(Task<?> task) {
                        return state.getTaskState(task);
                    }

                    @Override
                    public <T> T runChildTask(Task<T> task) throws MetadataDumperUsageException {
                        return runTask(this, state, task);
                    }
                };
                TASK:
                for (Task<?> task : tasks) {
                    runTask(runContext, state, task);
                }

            } finally {
                // We must do this in finally after the ZipFileSystem has been closed.
                if (outputFile.isFile())
                    LOG.debug("Dumper wrote " + outputFile.length() + " bytes.");
                LOG.debug("Dumper took " + stopwatch + ".");
            }

            int requiredTasksNotSucceeded = 0;
            System.out.println(
                    "**********************************************************\n"
                    + "* Task Summary:"
            );
            for (Map.Entry<Task<?>, TaskResult<?>> e : state.getTaskResultMap().entrySet()) {
                Task<? extends Object> task = e.getKey();
                TaskResult<? extends Object> result = e.getValue();
                StringBuilder buf = new StringBuilder();
                buf.append("* Task ").append(result.getState()).append(" (").append(task.getCategory()).append(")").append(' ').append(task);
                if (result.getException() != null)
                    buf.append(": ").append(result.getException());
                if (TaskCategory.REQUIRED.equals(task.getCategory()) && TaskState.FAILED.equals(result.getState()))
                    requiredTasksNotSucceeded++;
                System.out.println(buf.toString());
            }

            if (connector instanceof MetadataConnector) {
                System.out.println(
                        "**********************************************************\n"
                        + "* Metadata has been saved to " + outputFile + "\n"
                        + "**********************************************************"
                );
            } else if (connector instanceof LogsConnector) {
                System.out.println(
                        "**********************************************************\n"
                        + "* Logs have been saved to " + outputFile + "\n"
                        + "**********************************************************"
                );
            }

            if (requiredTasksNotSucceeded > 0) {
                System.out.println(
                        "* ERROR: " + requiredTasksNotSucceeded + " required task[s] failed.\n"
                        + "* Output, including debugging information, has been saved to " + outputFile + "\n"
                        + "**********************************************************"
                );
                if (exitOnError)
                    System.exit(1);
            }

        }
    }

    public static void main(String... args) throws Exception {
        try {
            MetadataDumper main = new MetadataDumper();
            args = JsonResponseFile.addResponseFiles(args);
            //LOG.debug("Arguments are: [" + String.join("] [", args) + "]");
            // Without this, the dumper prints "Missing required arguments:[connector]"
            if (args.length == 0)
                args = new String[]{"--help"};
            main.run(args);
        } catch (MetadataDumperUsageException e) {
            LOG.error(e.getMessage());
            for (String msg : e.getMessages())
                LOG.error(msg);
        }
    }
}
