
create local temp table tmp_observations
on commit preserve rows as 
(
	select 1 as user_id, x'0112a101024b01' as featurehash, 2 as cnt
	union all select 1, x'0112a101024b02', 1
	union all select 2, x'0112a1010310ff', 1
)
order by user_id 
segmented by hash(user_id) all nodes;


select throw_error('value for key ' || breakdownhash || ' does not match')
from (
	select to_hex(hash) as breakdownhash, sum(pv) as pv, sum(uv) as uv
	from (
	  select DistinctHashCounter(user_id, featurehash, cnt
	                             USING PARAMETERS
	                mask_header = x'01020304040505',
	                  mask_blob = x'ff0000ffffffff'||
	                              x'ffff00ffffffff'||
	                              x'ffffffffffffff'||
	                              x'ff0000ffff0000'||
	                              x'ffff00ffff0000'||
	                              x'ffffffffff0000'||
	                              x'ff000000000000'||
	                              x'ffff0000000000'||
	                              x'ffffff00000000')
	                over(partition auto order by user_id)
	  from tmp_observations
	) t
	group by 1
	minus
	(
		select '01120001020000',3,1
		union all select '01120001030000',1,1
		union all select '01000001020000',3,1
		union all select '01000001024b02',1,1
		union all select '011200010310ff',1,1
		union all select '0112a1010310ff',1,1
		union all select '0112a101024b01',2,1
		union all select '01000001024b01',2,1
		union all select '0112a101020000',3,1
		union all select '01120001024b01',2,1
		union all select '0112a101024b02',1,1
		union all select '0112a100000000',4,2
		union all select '01000001030000',1,1
		union all select '0112a101030000',1,1
		union all select '01000000000000',4,2
		union all select '010000010310ff',1,1
		union all select '01120001024b02',1,1
		union all select '01120000000000',4,2
	)
) diff;

