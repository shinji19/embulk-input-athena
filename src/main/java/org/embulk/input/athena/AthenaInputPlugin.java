package org.embulk.input.athena;

import com.google.common.base.Optional;

import java.io.File;
import java.io.FileFilter;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.jdbc.ToStringMap;
import org.embulk.plugin.PluginClassLoader;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.time.Timestamp;
import org.slf4j.Logger;

public class AthenaInputPlugin implements InputPlugin
{
    protected final Logger logger = Exec.getLogger(getClass());

    public interface PluginTask extends Task
    {
        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

        // database (required string)
        @Config("database")
        public String getDatabase();

        // athena_url (required string)
        @Config("athena_url")
        public String getAthenaUrl();

        // s3_staging_dir (required string)
        @Config("s3_staging_dir")
        public String getS3StagingDir();

        // access_key (required string)
        @Config("access_key")
        public String getAccessKey();

        // secret_key (required string)
        @Config("secret_key")
        public String getSecretKey();

        // query (required string)
        @Config("query")
        public String getQuery();

        // if you get schema from config
        @Config("columns")
        public SchemaConfig getColumns();

        @Config("options")
        @ConfigDefault("{}")
        public ToStringMap getOptions();

        @ConfigInject
        BufferAllocator getBufferAllocator();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = task.getColumns().toSchema();
        int taskCount = 1; // number of run() method calls

        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TaskReport run(TaskSource taskSource, Schema schema, int taskIndex, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        BufferAllocator allocator = task.getBufferAllocator();
        PageBuilder pageBuilder = new PageBuilder(allocator, schema, output);

        // Write your code here :)

        Connection connection = null;
        Statement statement = null;
        try {
            connection = getAthenaConnection(task);
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(task.getQuery());

            while (resultSet.next()) {
                schema.visitColumns(new ColumnVisitor()
                {
                    @Override
                    public void timestampColumn(Column column)
                    {
                        try {
                            java.sql.Timestamp t = resultSet.getTimestamp(column.getName());
                            pageBuilder.setTimestamp(column, Timestamp.ofEpochMilli(t.getTime()));
                        }
                        catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void stringColumn(Column column)
                    {
                        try {
                            pageBuilder.setString(column, resultSet.getString(column.getName()));
                        }
                        catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void longColumn(Column column)
                    {
                        try {
                            pageBuilder.setLong(column, resultSet.getLong(column.getName()));
                        }
                        catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void doubleColumn(Column column)
                    {
                        try {
                            pageBuilder.setDouble(column, resultSet.getDouble(column.getName()));
                        }
                        catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void booleanColumn(Column column)
                    {
                        try {
                            pageBuilder.setBoolean(column, resultSet.getBoolean(column.getName()));
                        }
                        catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void jsonColumn(Column column)
                    {
                        // TODO:
                    }
                });

                pageBuilder.addRecord();
            }
            pageBuilder.finish();

            pageBuilder.close();
            resultSet.close();
            connection.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            try {
                if (statement != null) {
                    statement.close();
                }
            }
            catch (Exception ex) { }
            try {
                if (connection != null) {
                    connection.close();
                }
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        return Exec.newTaskReport();
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }

    protected Connection getAthenaConnection(PluginTask task) throws ClassNotFoundException, SQLException
    {
        loadDriver("com.amazonaws.athena.jdbc.AthenaDriver", task.getDriverPath());
        Properties properties = new Properties();
        properties.put("s3_staging_dir", task.getS3StagingDir());
        properties.put("user", task.getAccessKey());
        properties.put("password", task.getSecretKey());
        properties.putAll(task.getOptions());

        return DriverManager.getConnection(task.getAthenaUrl(), properties);
    }

    //
    // copy from embulk-input-jdbc
    //

    protected void loadDriver(String className, Optional<String> driverPath)
    {
        if (driverPath.isPresent()) {
            addDriverJarToClasspath(driverPath.get());
        }
        else {
            try {
                // Gradle test task will add JDBC driver to classpath
                Class.forName(className);
            }
            catch (ClassNotFoundException ex) {
                File root = findPluginRoot();
                File driverLib = new File(root, "default_jdbc_driver");
                File[] files = driverLib.listFiles(new FileFilter() {
                    @Override
                    public boolean accept(File file)
                    {
                        return file.isFile() && file.getName().endsWith(".jar");
                    }
                });
                if (files == null || files.length == 0) {
                    throw new RuntimeException("Cannot find JDBC driver in '" + root.getAbsolutePath() + "'.");
                }
                else {
                    for (File file : files) {
                        logger.info("JDBC Driver = " + file.getAbsolutePath());
                        addDriverJarToClasspath(file.getAbsolutePath());
                    }
                }
            }
        }

        // Load JDBC Driver
        try {
            Class.forName(className);
        }
        catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected void addDriverJarToClasspath(String glob)
    {
        // TODO match glob
        PluginClassLoader loader = (PluginClassLoader) getClass().getClassLoader();
        Path path = Paths.get(glob);
        if (!path.toFile().exists()) {
             throw new ConfigException("The specified driver jar doesn't exist: " + glob);
        }
        loader.addPath(Paths.get(glob));
    }

    protected File findPluginRoot()
    {
        try {
            URL url = getClass().getResource("/" + getClass().getName().replace('.', '/') + ".class");
            if (url.toString().startsWith("jar:")) {
                url = new URL(url.toString().replaceAll("^jar:", "").replaceAll("![^!]*$", ""));
            }

            File folder = new File(url.toURI()).getParentFile();
            for (;; folder = folder.getParentFile()) {
                if (folder == null) {
                    throw new RuntimeException("Cannot find 'embulk-input-xxx' folder.");
                }

                if (folder.getName().startsWith("embulk-input-")) {
                    return folder;
                }
            }
        }
        catch (MalformedURLException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
