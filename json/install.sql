CREATE or replace LIBRARY RapidArray AS '/tmp/RapidArray.so' LANGUAGE 'C++';
CREATE or replace TRANSFORM FUNCTION RapidArrayExtractor AS LANGUAGE 'C++' NAME 'RapidArrayFactory' LIBRARY RapidArray;
GRANT ALL ON LIBRARY RapidArray TO public;
CREATE or replace LIBRARY RapidJson AS '/tmp/RapidJson.so' LANGUAGE 'C++';
CREATE or replace TRANSFORM FUNCTION RapidJsonExtractor AS LANGUAGE 'C++' NAME 'RapidJsonFactory' LIBRARY RapidJson;
GRANT ALL ON LIBRARY RapidJson TO public;
GRANT all ON ALL FUNCTIONS IN SCHEMA public to public;

