package org.apache.flink.sql.examples;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.nio.charset.Charset;

/**
 * @author zlz
 * @Description
 * @since 2021/8/5 15:30
 */
public class FlinkSql {
    private final static Charset CHARSET = Charset.forName("UTF-8");

    public static void main(String[] args) throws Exception {
//        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        env.getConfig().setGlobalJobParameters(params);
//        //构建EnvironmentSettings 并指定Blink Planner
//        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//        final StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, bsSettings);
//        final SQLExecutor executor = new SQLExecutor(tableEnvironment);
//        String sql = params.get("sql");
//        String name = null;
//        if (StringUtils.isBlank(sql)) {
//            String nameVal = params.get("name");
//            name = nameVal;
//            String pathVal = params.get("path", "/etc/flink/sql");
//            String path1 = FlinkSql.class.getClassLoader().getResource("").getPath();
//            int pos = path1.indexOf("target");
//            pathVal = path1.substring(0, pos);
//            pathVal = pathVal+"src/test/resources/";
//            nameVal = "test";
//            Path path = new Path(pathVal, nameVal + ".sql");
//            try (FSDataInputStream inputStream = path.getFileSystem().open(path)) {
//                sql = IOUtils.toString(inputStream, CHARSET);
//            }
//        }
//        executor.execute(sql);
////        tableEnvironment.from("test").getSchema();
//        env.execute(name == null ? FlinkSql.class.getSimpleName() : name);
        StreamExecutionEnvironment env;
        StreamTableEnvironment tEnv;
        SQLExecutor executor;
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        //构建EnvironmentSettings 并指定Blink Planner
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        tEnv = StreamTableEnvironment.create(env, bsSettings);
        executor = new SQLExecutor(tEnv);
        //通过DDL，注册kafka数据源表
        tEnv.sqlUpdate(FlinkSqlConstant.KAFKA_TABLE_SOURCE_DDL);

        //通过DDL，注册mysql数据结果表
        tEnv.sqlUpdate(FlinkSqlConstant.MYSQL_TABLE_SINK_DDL);

        //将从kafka中查到的数据，插入mysql中
        tEnv.sqlUpdate("insert into user_behavior_mysql select user_id,item_id from user_behavior limit 1");

        env.execute("kafkaSourceTest");
    }

    public static void kafkaSource() throws Exception{
        StreamExecutionEnvironment env;
        StreamTableEnvironment tEnv;
        SQLExecutor executor;
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        //构建EnvironmentSettings 并指定Blink Planner
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        tEnv = StreamTableEnvironment.create(env, bsSettings);
        executor = new SQLExecutor(tEnv);
        //通过DDL，注册kafka数据源表
        tEnv.sqlUpdate(FlinkSqlConstant.KAFKA_TABLE_SOURCE_DDL);

        //通过DDL，注册mysql数据结果表
        tEnv.sqlUpdate(FlinkSqlConstant.MYSQL_TABLE_SINK_DDL);

        //将从kafka中查到的数据，插入mysql中
        tEnv.sqlUpdate("insert into user_behavior_mysql select user_id,item_id from user_behavior limit 1");

        env.execute("kafkaSourceTest");
    }
}
