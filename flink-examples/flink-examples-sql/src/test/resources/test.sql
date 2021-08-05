

CREATE FUNCTION test AS "com.hikvision.ga.hrule.flink.udf.MyScalarFunc";
CREATE TABLE test
(
    user_id     bigint,
    item_id     bigint,
    category_id bigint,
    behavior    VARCHAR
) WITH (
    'connector.type' = 'filesystem',-- 指定连接类型
    'format.type' = 'csv',
    'connector.property-version' = '1',
    'format.property-version' = '1',
    'update-mode' = 'append',
    'connector.path' = './test.csv',
    'format.field-delimiter' = ',' , -- 字段分隔符
    'format.fields.0.name' = 'user_id',-- 第N字段名，相当于表的schema，索引从0开始
    'format.fields.0.data-type' = 'bigint',-- 字段类型
    'format.fields.1.name' = 'item_id',
    'format.fields.1.data-type' = 'bigint',
    'format.fields.2.name' = 'category_id',
    'format.fields.2.data-type' = 'bigint',
    'format.fields.3.name' = 'behavior',
    'format.fields.3.data-type' = 'String'
);
CREATE TABLE bbb
(
    user_id     bigint,
    item_id     bigint,
    category_id bigint,
    behavior    VARCHAR
) WITH (
    'connector.type' = 'filesystem',
    'format.type' = 'csv',
    'connector.property-version' = '1',
    'format.property-version' = '1',
    'update-mode' = 'append',
    'connector.path' = './bbb.csv',
    'format.field-delimiter' = ':' ,
    'format.fields.0.name' = 'user_id',
    'format.fields.0.data-type' = 'bigint',
    'format.fields.1.name' = 'item_id',
    'format.fields.1.data-type' = 'bigint',
    'format.fields.2.name' = 'category_id',
    'format.fields.2.data-type' = 'bigint',
    'format.fields.3.name' = 'behavior',
    'format.fields.3.data-type' = 'String'
);

CREATE TABLE ccc
(
    user_id     bigint,
    item_id     bigint,
    category_id bigint,
    behavior    VARCHAR
) WITH (
    'connector.type' = 'filesystem',
    'format.type' = 'csv',
    'connector.property-version' = '1',
    'format.property-version' = '1',
    'update-mode' = 'append',
    'connector.path' = './ccc.csv',
    'format.field-delimiter' = ',' ,
    'format.fields.0.name' = 'user_id',
    'format.fields.0.data-type' = 'bigint',
    'format.fields.1.name' = 'item_id',
    'format.fields.1.data-type' = 'bigint',
    'format.fields.2.name' = 'category_id',
    'format.fields.2.data-type' = 'bigint',
    'format.fields.3.name' = 'behavior',
    'format.fields.3.data-type' = 'String'
);

INSERT INTO bbb
SELECT *
FROM test;

INSERT INTO ccc
SELECT test(user_id),item_id,category_id,behavior FROM test;
