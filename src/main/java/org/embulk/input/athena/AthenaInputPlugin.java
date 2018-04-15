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
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Types;

public class AthenaInputPlugin
        implements InputPlugin
{
    public interface PluginTask
            extends Task, TimestampParser.Task
    {
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

        // interval (required int)
        @Config("check_interval")
        @ConfigDefault("1000")
        public int getCheckInterval();

        // configuration option 2 (optional string, null is not allowed)
        // @Config("option2")
        // @ConfigDefault("\"myvalue\"")
        // public String getOption2();

        // configuration option 3 (optional string, null is allowed)
        // @Config("option3")
        // @ConfigDefault("null")
        // public Optional<String> getOption3();

        // if you get schema from config
        // @Config("columns")
        // public SchemaConfig getColumns();

        @Config("columns")
        @ConfigDefault("[]")
        public List<ColumnOption> getColumns();

        @ConfigInject
        BufferAllocator getBufferAllocator();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // Schema schema = task.getColumns().toSchema();
        Schema schema = Schema.builder().build();
        int taskCount = 1;  // number of run() method calls

        schema = Schema.builder()
            .add("created_at", Types.STRING)
            .add("uid", Types.STRING)
            .add("logtype", Types.STRING)
            .add("device_os", Types.STRING)
            .build();

        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TaskReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
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

            ResultSetMetaData m = resultSet.getMetaData();
            while(resultSet.next()){
                for (int i = 0; i < m.getColumnCount(); i++){
                    String colName = m.getColumnName(i + 1);
                    // String className = m.getColumnClassName(i + 1);

                    schema.visitColumns(new ColumnVisitor(){
                        @Override
                        public void timestampColumn(Column column){
                            try {
                                TimestampParser parser = TimestampParser.of(task, task.getColumns().get(column.getIndex()));
                                Timestamp t = parser.parse(resultSet.getString(colName));
    							pageBuilder.setTimestamp(column, t);
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        }

                        @Override
						public void stringColumn(Column column) {
                            try {
    							pageBuilder.setString(column, resultSet.getString(colName));
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
						}

						@Override
						public void longColumn(Column column) {
                            try {
    							pageBuilder.setLong(column, resultSet.getLong(colName));
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
						}

						@Override
						public void doubleColumn(Column column) {
                            try {
    							pageBuilder.setDouble(column, resultSet.getDouble(colName));
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
						}

						@Override
						public void booleanColumn(Column column) {
                            try {
    							pageBuilder.setBoolean(column, resultSet.getBoolean(colName));
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        }
                        
                        @Override
                        public void jsonColumn(Column column) {
                            // TODO:
                        }
                    });
                    
                    //Column c = schema.getColumn(i);
                    //pageBuilder.setString(c, resultSet.getString(colName));
                    pageBuilder.flush();

                }
                pageBuilder.addRecord();
                pageBuilder.flush();
            }
            pageBuilder.finish();
            pageBuilder.flush();
            
            pageBuilder.close();
            resultSet.close();
            connection.close();
        } catch (Exception e){
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
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }

    protected Connection getAthenaConnection(PluginTask task) throws ClassNotFoundException, SQLException{
        Class.forName("com.amazonaws.athena.jdbc.AthenaDriver");
        Properties properties = new Properties();
        properties.put("s3_staging_dir", task.getS3StagingDir());
        properties.put("user", task.getAccessKey());
        properties.put("password", task.getSecretKey());

        return DriverManager.getConnection(task.getAthenaUrl(), properties);
    }

    interface ColumnOption extends Task, TimestampParser.TimestampColumnOption
    {
    }
}
