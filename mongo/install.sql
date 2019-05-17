
CREATE OR REPLACE LIBRARY MongoConnector AS '/tmp/MongoConnector.so';
CREATE OR REPLACE SOURCE MongodbSource AS LANGUAGE 'C++' NAME 'MongodbSourceFactory' LIBRARY MongoConnector;
grant all on library MongoConnector to public;
GRANT all ON ALL FUNCTIONS IN SCHEMA public to public;

