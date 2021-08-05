package org.apache.flink.sql.examples;

/**
 * @author zlz
 * @Description
 * @since 2021/8/5 15:30
 */
public class FlinkSqlConstant {
    public static final String KAFKA_TABLE_SOURCE_DDL = "" +
        "CREATE TABLE user_behavior (\n" +
        "    user_id BIGINT,\n" +
        "    item_id BIGINT\n" +
        ") WITH (\n" +
        "    'connector.type' = 'kafka',  -- 指定连接类型是kafka\n" +
        "    'connector.version' = '0.11',  -- kafka版本\n" +
        "    'connector.topic' = 'test', -- 之前创建的topic \n" +
        "    'connector.properties.group.id' = 'flink-test-0', -- 消费者组，相关概念可自行百度\n" +
//            "    'connector.startup-mode' = 'earliest-offset',  --指定从最早消费\n" +
        "    'connector.properties.zookeeper.connect' = '10.19.154.131:2181',  -- zk地址\n" +
        "    'connector.properties.bootstrap.servers' = '10.19.154.131:9092',  -- broker地址\n" +
        "    'format.type' = 'json'  -- json格式，和topic中的消息格式保持一致\n" +
        ")";

    public static final String MYSQL_TABLE_SINK_DDL = "" +
        "CREATE TABLE `user_behavior_mysql` (\n" +
        "  `user_id` bigint  ,\n" +
        "  `item_id` bigint \n" +
        ")WITH (\n" +
        "  'connector.type' = 'jdbc', -- 连接方式\n" +
        "  'connector.url' = 'jdbc:mysql://10.19.154.131:3306/test', -- jdbc的url\n" +
        "  'connector.table' = 'user_behavior',  -- 表名\n" +
        "  'connector.driver' = 'com.mysql.cj.jdbc.Driver', -- 驱动名字，可以不填，会自动从上面的jdbc url解析 \n" +
        "  'connector.username' = 'root', -- 顾名思义 用户名\n" +
        "  'connector.password' = 'mysql' , -- 密码\n" +
        "  'connector.write.flush.max-rows' = '5000', -- 意思是攒满多少条才触发写入 \n" +
        "  'connector.write.flush.interval' = '2s' -- 意思是攒满多少秒才触发写入；这2个参数，无论数据满足哪个条件，就会触发写入\n" +
        ")";
}
