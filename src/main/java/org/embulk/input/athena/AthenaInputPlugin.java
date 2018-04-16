package org.embulk.input.athena;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.jdbc.ToStringMap;
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

public class AthenaInputPlugin implements InputPlugin {
    public interface PluginTask extends Task {
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
    public ConfigDiff transaction(ConfigSource config, InputPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = task.getColumns().toSchema();
        int taskCount = 1; // number of run() method calls

        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, InputPlugin.Control control) {
        control.run(taskSource, schema, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports) {
    }

    @Override
    public TaskReport run(TaskSource taskSource, Schema schema, int taskIndex, PageOutput output) {
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

            ResultSetMetaData m = resultSet.getMetaData();
            while (resultSet.next()) {
                schema.visitColumns(new ColumnVisitor() {
                    @Override
                    public void timestampColumn(Column column) {
                        try {
                            java.sql.Timestamp t = resultSet.getTimestamp(column.getName());
                            pageBuilder.setTimestamp(column, Timestamp.ofEpochMilli(t.getTime()));
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void stringColumn(Column column) {
                        try {
                            pageBuilder.setString(column, resultSet.getString(column.getName()));
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void longColumn(Column column) {
                        try {
                            pageBuilder.setLong(column, resultSet.getLong(column.getName()));
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void doubleColumn(Column column) {
                        try {
                            pageBuilder.setDouble(column, resultSet.getDouble(column.getName()));
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void booleanColumn(Column column) {
                        try {
                            pageBuilder.setBoolean(column, resultSet.getBoolean(column.getName()));
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void jsonColumn(Column column) {
                        // TODO:
                    }
                });

                pageBuilder.addRecord();
            }
            pageBuilder.finish();

            pageBuilder.close();
            resultSet.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (statement != null)
                    statement.close();
            } catch (Exception ex) {

            }
            try {
                if (connection != null)
                    connection.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        return Exec.newTaskReport();
    }

    @Override
    public ConfigDiff guess(ConfigSource config) {
        return Exec.newConfigDiff();
    }

    protected Connection getAthenaConnection(PluginTask task) throws ClassNotFoundException, SQLException {
        Class.forName("com.amazonaws.athena.jdbc.AthenaDriver");
        Properties properties = new Properties();
        properties.put("s3_staging_dir", task.getS3StagingDir());
        properties.put("user", task.getAccessKey());
        properties.put("password", task.getSecretKey());
        properties.putAll(task.getOptions());

        return DriverManager.getConnection(task.getAthenaUrl(), properties);
    }
}
