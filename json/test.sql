
create local temp table tmp_json 
on commit preserve rows as 
(
  select 1 as id, '{"a": 1, "b": "a", "c": false, "d": null, "e": [{"f": 5, "g": 6}] }' as j
  union all select 2, '{"a": 0, "c": true, "e": []}'
)
order by id 
unsegmented all nodes;


select throw_error('json fields do not match')
from (
	select RapidJsonExtractor(id,j using parameters keys='a;b;e') over(partition auto) 
	from tmp_json
	minus ( 
		select 1,'1','a','[{"f":5,"g":6}]' 
		union all select 2,'0',null,'[]'
	)
) t;


create local temp table tmp_array 
on commit preserve rows as 
(
  select 1 as id, '[1,2,3,4]' as a
  union all select 2, '[5,6]'
)
order by id 
unsegmented all nodes;


select throw_error('array elements do not match')
from (
	select RapidArrayExtractor(id,a) over(partition auto)
	from tmp_array
	minus (
		select 1,1,'1'
		union all select 1,2,'2' 
		union all select 1,3,'3' 
		union all select 1,4,'4' 
		union all select 2,1,'5' 
		union all select 2,2,'6' 
	)
) t;

