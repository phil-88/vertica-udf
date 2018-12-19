CREATE or replace LIBRARY DistinctHash AS '/tmp/DistinctHash.so' LANGUAGE 'C++';
CREATE or replace TRANSFORM FUNCTION DistinctHashCounter AS LANGUAGE 'C++' NAME 'DistinctHashCounterFactory' LIBRARY DistinctHash;
CREATE or replace TRANSFORM FUNCTION DistinctHashCounter AS LANGUAGE 'C++' NAME 'DistinctHashCounterFactory2' LIBRARY DistinctHash;
CREATE or replace FUNCTION TUPLE_TO_BINARY AS LANGUAGE 'C++' NAME 'Int2BinaryFactory' LIBRARY DistinctHash;
GRANT ALL ON LIBRARY DistinctHash TO public;

create or replace function TO_BINARY(i int, sz int) 
return varbinary(4)
as 
begin
	return hex_to_binary(lpad(to_hex(coalesce(i & ((1 << 8 * sz) - 1),((1 << 8 * sz) - 1))),sz*2,'0'));	
end;

GRANT all ON ALL FUNCTIONS IN SCHEMA public to public;
