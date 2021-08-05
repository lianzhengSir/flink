
CREATE TABLE source_table
(
    IP     VARCHAR,
    ttime    VARCHAR
) WITH (
    'connector.type' = 'filesystem',-- 指定连接类型
    'format.type' = 'csv',
    'connector.property-version' = '1',
    'format.property-version' = '1',
    'update-mode' = 'append',
    'connector.path' = './testTopN.csv',
    'format.field-delimiter' = ',' , -- 字段分隔符
    'format.fields.0.name' = 'IP',-- 第N字段名，相当于表的schema，索引从0开始
    'format.fields.0.data-type' = 'String',-- 字段类型
    'format.fields.1.name' = 'ttime',
    'format.fields.1.data-type' = 'String'
);

CREATE TABLE result_table
(
  rownum BIGINT,
  start_time VARCHAR,
  IP VARCHAR,
  cc BIGINT
--   PRIMARY KEY (start_time, IP)
) WITH (
    'connector.type' = 'jdbc', -- 连接方式
    'connector.url' = 'jdbc:mysql://local:3306/test', -- jdbc的url
    'connector.table' = 'out',  -- 表名
    'connector.driver' = 'com.mysql.cj.jdbc.Driver', -- 驱动名字，可以不填，会自动从上面的jdbc url解析
    'connector.username' = 'root', -- 顾名思义 用户名
    'connector.password' = 'mysql' , -- 密码
    'connector.write.flush.max-rows' = '5000', -- 意思是攒满多少条才触发写入
    'connector.write.flush.interval' = '1s' -- 意思是攒满多少秒才触发写入；这2个参数，无论数据满足哪个条件，就会触发写入
);

INSERT INTO result_table
SELECT rownum,start_time,IP,cc
FROM (
  SELECT *,
     ROW_NUMBER() OVER (PARTITION BY start_time ORDER BY cc DESC) AS rownum
  FROM (
        SELECT SUBSTRING(ttime,1,2) AS start_time,--可以根据真实时间取相应的数值，这里取得是测试数据。
        COUNT(IP) AS cc,
        IP
        FROM  source_table
        GROUP BY SUBSTRING(ttime,1,2), IP
    )a
) t
WHERE rownum <= 3;
