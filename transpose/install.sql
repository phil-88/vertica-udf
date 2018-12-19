CREATE or replace LIBRARY Transpose AS '/tmp/Transpose.so' LANGUAGE 'C++';
CREATE or replace TRANSFORM FUNCTION Transpose AS LANGUAGE 'C++' NAME 'TransposeFactory' LIBRARY Transpose;
GRANT ALL ON LIBRARY Transpose TO public;
GRANT all ON ALL FUNCTIONS IN SCHEMA public to public;

