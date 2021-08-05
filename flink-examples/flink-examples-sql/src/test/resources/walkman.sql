CREATE TABLE snap_image_info
(
    device_id  VARCHAR,
    human_id   VARCHAR,
    trace_uuid VARCHAR,
    face_time  TIMESTAMP(3),
    WATERMARK FOR face_time AS face_time - INTERVAL '5' SECOND
) WITH (
--     'connector.type' = 'kafka',
    'connector.type' = 'filesystem',
    'format.type' = 'csv',
    'connector.property-version' = '1',
    'format.property-version' = '1',
    'update-mode' = 'append',-- otherwise: 'retract' or 'upsert'
    'connector.path' = './walkman.csv'
);

CREATE VIEW T1 AS
SELECT device_id,
       COLLECT(CONCAT(human_id, cast(face_time as VARCHAR)))[1]            as human_ids,
       HOP_START(face_time, INTERVAL '10' SECOND, INTERVAL '5' SECOND) as start_time,
       HOP_END(face_time, INTERVAL '10' SECOND, INTERVAL '5' SECOND)   as end_time
FROM snap_image_info
GROUP BY HOP(face_time, INTERVAL '10' SECOND, INTERVAL '5' SECOND),
         device_id
;

CREATE TABLE walk_man
(
    device_id  VARCHAR,
    human_ids  VARCHAR,
    start_time TIMESTAMP(3),
    end_time   TIMESTAMP(3)
) WITH (
    'connector.type' = 'filesystem',
    'format.type' = 'csv',
    'connector.property-version' = '1',
    'format.property-version' = '1',
    'update-mode' = 'append',
    'connector.path' = './resources/w1.csv'
);

INSERT INTO walk_man
select *
from T1
;


-- INSERT INTO walk_man
-- select face_time, device_id, v1, v2
-- from T1,
--      lateral table (aaa(human_ids, ',')) as T(v1, v2)
