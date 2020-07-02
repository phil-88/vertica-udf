
\set ON_ERROR_STOP on;

select throw_error('cannot install over running query')
from query_requests
where session_id <> current_session() and is_executing and request ilike '%KafkaSparseSource%';

CREATE OR REPLACE LIBRARY KafkaConnector AS '/tmp/KafkaConnector.so';
CREATE OR REPLACE SOURCE KafkaSparseSource AS LANGUAGE 'C++' NAME 'KafkaSourceFactory' LIBRARY KafkaConnector;
grant all on library KafkaConnector to public;
GRANT all ON ALL FUNCTIONS IN SCHEMA public to public;


