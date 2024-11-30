CREATE TABLE source_db.kafka_stream_config (
    topic_name VARCHAR(255) NOT NULL,
    starting_offsets VARCHAR(50) DEFAULT 'latest',
    schema_id VARCHAR(50) NOT NULL,
    output_path VARCHAR(255) NOT NULL,
    checkpoint_path VARCHAR(255) NOT NULL,
    trigger_interval VARCHAR(50) DEFAULT '10 seconds',
    output_mode VARCHAR(20) DEFAULT 'append',
    await_termination_duration VARCHAR(50) DEFAULT NULL,
    PRIMARY KEY (topic_name, schema_id)
);

alter table source_db.kafka_stream_config modify await_termination_duration int;

select * from source_db.kafka_stream_config;

truncate table source_db.kafka_stream_config;
INSERT INTO source_db.kafka_stream_config (
    topic_name, starting_offsets, schema_id,
    output_path, checkpoint_path, trigger_interval, output_mode, await_termination_duration
) VALUES
    ('voter_topic', 'earliest', 2,
     'src/parquetLocation/voter', 'src/checkpoints/voter', '10 seconds', 'append', '3600');

INSERT INTO source_db.kafka_stream_config (
    topic_name, starting_offsets, schema_id,
    output_path, checkpoint_path, trigger_interval, output_mode, await_termination_duration
) VALUES
    ('votes_topic', 'latest', 3,
     'src/parquetLocation/vote', 'src/checkpoints/vote', '10 seconds', 'append', '50');

INSERT INTO source_db.kafka_stream_config (
    topic_name, starting_offsets, schema_id,
    output_path, checkpoint_path, trigger_interval, output_mode, await_termination_duration
) VALUES
    ('candidate_topic', 'latest', 3,
     'src/parquetLocation/candidate', 'src/checkpoints/candidate', '10 seconds', 'append', '3600');





select * from source_db.kafka_stream_config;
select * from source_db.schema_config;

update source_db.kafka_stream_config set schema_id = '3' where topic_name = 'candidate_topic';


delete from source_db.kafka_stream_config where topic_name = 'votes_topic';

select * from source_db.kafka_stream_config;

CREATE TABLE source_db.schema_config (
    schema_id VARCHAR(50) PRIMARY KEY,
    topicSchema TEXT NOT NULL
);


select * from source_db.kafka_stream_config;


truncate table source_db.schema_config;
INSERT INTO source_db.schema_config (schema_id, topicSchema) VALUES
    ('1', '{"type":"struct","fields":[{"name":"candidate_id","type":"string","nullable":false},{"name":"candidate_name","type":"string","nullable":true},{"name":"party_affiliation","type":"string","nullable":true},{"name":"biography","type":"string","nullable":true},{"name":"campaign_platform","type":"string","nullable":true},{"name":"photo_url","type":"string","nullable":true}]}');


INSERT INTO source_db.schema_config (schema_id, topicSchema) VALUES
    ('2', '{
      "type": "struct",
      "fields": [
        {"name": "voter_id", "type": "string", "nullable": true},
        {"name": "voter_name", "type": "string", "nullable": true},
        {"name": "date_of_birth", "type": "string", "nullable": true},
        {"name": "gender", "type": "string", "nullable": true},
        {"name": "nationality", "type": "string", "nullable": true},
        {"name": "registration_number", "type": "string", "nullable": true},
        {"name": "address_street", "type": "string", "nullable": true},
        {"name": "address_city", "type": "string", "nullable": true},
        {"name": "address_state", "type": "string", "nullable": true},
        {"name": "address_country", "type": "string", "nullable": true},
        {"name": "address_postcode", "type": "string", "nullable": true},
        {"name": "email", "type": "string", "nullable": true},
        {"name": "phone_number", "type": "string", "nullable": true},
        {"name": "cell_number", "type": "string", "nullable": true},
        {"name": "picture", "type": "string", "nullable": true},
        {"name": "registered_age", "type": "integer", "nullable": true}
      ]
    }');


INSERT INTO source_db.schema_config (schema_id, topicSchema) VALUES
    ('3', '{"type":"struct","fields":[{"name":"voter_id","type":"string","nullable":false},{"name":"candidate_id","type":"string","nullable":true},{"name":"voting_time","type":"timestamp","nullable":true},{"name":"vote","type":"integer","nullable":true}]}');

select * from source_db.schema_config;

