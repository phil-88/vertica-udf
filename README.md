# vertica-udf

Contains c++ transform functions for fast data processing on Vertica OLAP database:
* RapidJsonExtractor - fast sax-based json-to-columns parser using rapid_json library.
* RapidArrayExtractor - same for simple arrays.
* DistinctHashCounter - pipelined group by for many dimention combinations in single run.
* Transpose - column-to-row transpose.

Best performance is archived on over(partition auto) clause.

## examples
### RapidJsonExtractor

```
create local temp table tmp_json 
on commit preserve rows as 
(
  select 1 as id, '{"a": 1, "b": "a", "c": false, "d": null, "e": [{"f": 5, "g": 6}] }' as j
  union all select 2, '{"a": 0, "c": true, "e": []}'
)
order by id 
unsegmented all nodes;
```

```
select * from tmp_json;

id |j                                                                   |
---|--------------------------------------------------------------------|
1  |{"a": 1, "b": "a", "c": false, "d": null, "e": [{"f": 5, "g": 6}] } |
2  |{"a": 0, "c": true, "e": []}                                        |
```

```
select RapidJsonExtractor(id,j using parameters keys='a;b;e') over(partition auto) from tmp_json;

id |a |b |e               |
---|--|--|----------------|
1  |1 |a |[{"f":5,"g":6}] |
2  |0 |  |[]              |
```

### RapidArrayExtractor

```
create local temp table tmp_array 
on commit preserve rows as 
(
  select 1 as id, '[1,2,3,4]' as a
  union all select 2, '[5,6]'
)
order by id 
unsegmented all nodes;
```

```
select * from tmp_array;

id |a         |
---|----------|
1  |[1,2,3,4] |
2  |[5,6]     |
```

```
select RapidArrayExtractor(id,a) over(partition auto) from tmp_array;

id |rnk |value |
---|----|------|
1  |1   |1     |
1  |2   |2     |
1  |3   |3     |
1  |4   |4     |
2  |1   |5     |
2  |2   |6     |
```

### DistinctHashCounter
```
create local temp table tmp_observations
on commit preserve rows as 
(
	select 1 as user_id, x'0012a101024b01' as featurehash, 2 as cnt
	union all select 1, x'0012a101024b02', 1
)
order by user_id 
segmented by hash(user_id) all nodes;
```

Event feature hash explained:
```
     00      12      a1    0102    4b01
\______/\______/\______/\______/\______/
 isnew   region  city    categ   subcat
```

```
select to_hex(hash) as breakdownhash, pv, uv
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
```
