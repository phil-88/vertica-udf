# vertica-udf

Contains c++ transform functions for fast data processing on Vertica OLAP database:
* RapidJsonExtractor - fast sax-based json-to-columns parser using rapid_json library.
* RapidArrayExtractor - same for simple arrays.
* DistinctHashCounter - pipelined group by for all possible combinations of dimensions in single run.
* Transpose - column-to-row transpose.
* MongodbSource - mongodb copy connector.
* KafkaSparseSource - kafka copy connector.

Best performance is achieved using over(partition auto) clause.

## Use cases 
Vertica has many great built in functions suited for general purposes. But there are extreme cases they cannot cope with.

### json/array unpivot
Built in mapvalues(VMapJsonExtractor()) or maplookup(VMapJsonExtractor()) works good for extracting few columns from json. But they are too slow when you need to extract 100+ columns in a wide table. **RapidJsonExtractor** is designed to extract all top level fields from simple json in a single run for reasonable time (few minutes for extracting 600 fields from 10 million records on vertica with 20 nodes).

The same for mapvalues(VMapDelimitedExtractor()). If you need to unpivot 1 billion values from 50 million arrays (20 items from a single record) using VMapDelimitedExtractor it takes like forever. **RapidArrayExtractor** process such a task in a few minutes on a 20 node vertica.

### massive count distinct
Vertica implements extremely efficient pipeleined group by algorithm to calculate distinct counts. It takes an advantage of data order so it just makes +1 when target id column increases instead of maintaining hash table of distinct values. To make it work you need to have you data ordered by dimension columns first and target id column as a last order column. It works great if you have data ordered in a such manner. So when you need to calculate distinct count of users in all possible combinations of dimensions you need to prepopulate and preorder all combinations out of fact table. And if you have 100 million fact table you end up with a trillion record table of combinations. It is like insane. 

So I came up with **DistinctHashCounter** which combines pipeline on target id column and hash table on dimension columns. You just have to order your fact table by target id column. It maintains hash table of dimensions and stores current target value for each dimension tuple. So at each iteration it just searches for corresponding record in a dimension hash table, makes +1 and updates current target value. So you have the same exact pipelined algorithm for each dimension in a hash table. With DistinctHashCounter you can calculate unique visitors from 1 billion fact table in 10 million combinations of dimensions for 15 minutes on 20 node vertica.

Algorithm can be described as follows:
1. for each event concatenate dimensions in a single feature hash along with target id (user_id or cookie_id)
```
	target_id = user_id
	feature_hash = dim1 || dim2 || dim3 || dim4
```
2. expose each feature hash to the set of masks to generate all possible accumulator products
```
	accumulator_product = feature_hash * accumulator_mask
```
3. maintain hash table of accumulators with last seen target id and counter value
* if target id from current event equals to last seen id for given accumulator continue with next accumulator product
* otherwise increase counter for accumulator product, save target id as last seen, continue with next accumulator product
```
	accumulators[accumulator_product].target_id = target_id
	accumulators[accumulator_product].counter += 1
```
4. print hash table

Detailed description is available here:
Golov, Filatov, Bruskin "Efficient Exact Algorithm for Count Distinct Problem", Springer, Computer Algebra in Scientific Computing, LNCS 11661

### load documents from MongoDB 
There is a build-in flex extension for loading documents in vertica. And there are two problems with it:
* it is slow to parse on copy (at least two times slower then just insert document as is in a single varchar column)
* it is slow to get too many values out of vmap representation in a single run

So you can combine reading and extracting columns inside copy source using native mongodb driver. It runs inside vertica itself (eliminating transfer from app server) and works realy fast. We get it to work 5 times faster than python to copy from local stdin based extractor we used before. So now we have 1 million per minute ratio for 500 byte documents on a single worker.

### Kafka connector
Yet another Kafka connector for vertica with open source code. Optimized to load jsons with 1500 column schema at 2Mrows/min.


## Examples
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
	select 1 as user_id, x'0112a101024b01' as featurehash, 2 as cnt
	union all select 1, x'0112a101024b02', 1
	union all select 2, x'0112a1010310ff', 1
)
order by user_id 
segmented by hash(user_id) all nodes;
```

Event feature hash explained for x'0112a101024b01':
```
     01      12      a1    0102    4b01
\______/\______/\______/\______/\______/
 platf   region  city    categ   subcat
```

Event feature mask explained for x'ffff00ffff0000':
```
     ff      ff      00    ffff    0000
  as-is   as-is     any   as-is     any
\______/\______/\______/\______/\______/
 platf   region  city    categ   subcat
```
We use zeroes to build hierarchy of dimentions as follow
```
ffff - region and city from event without changes
ff00 - region from event, any city
0000 - any region, any city
```

```
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
group by 1;

to_hex         |pv |uv 
---------------|---|---
01000000000000 |4  |2  <- 2 users on plafrom 1, any region, any city, any category, any subcategory
01000001020000 |3  |1  
01000001024b01 |2  |1  
01000001024b02 |1  |1  
01000001030000 |1  |1  
010000010310ff |1  |1  
01120000000000 |4  |2  <- 2 users on plafrom 1, in region 12, any city, any category, any subcategory
01120001020000 |3  |1  
01120001024b01 |2  |1  
01120001024b02 |1  |1  
01120001030000 |1  |1  
011200010310ff |1  |1  
0112a100000000 |4  |2  <- 2 users on plafrom 1, in region 12, city a1, any category, any subcategory
0112a101020000 |3  |1  
0112a101024b01 |2  |1  
0112a101024b02 |1  |1  
0112a101030000 |1  |1  
0112a1010310ff |1  |1  
```

### MongodbSource

For raw json:
```
create local temp table tmp_mongo_json 
(
	id varchar(32), 
	doc varchar(32000)
)
on commit preserve rows
order by id 
segmented by hash(id) all nodes;
```

```
COPY tmp_mongo_json 
WITH SOURCE MongodbSource
(                                 
	url='mongodb://host1:27017,host2:27017/?replicaSet=rs01',
	database='dwh',
	collection='clickstream',
	query='{"_id": {"$lte": {"$oid": "5cde76f7a90e7ba133edbae1"}, "$gt": {"$oid": "5cde74d3a90e7ba1339e73ff"}}}',
	format='json',
	delimiter=E'\001',                 
	terminator=E'\002'
)                
DELIMITER E'\001' RECORD TERMINATOR E'\002' NO ESCAPE DIRECT
REJECTMAX 0 ABORT ON ERROR;
```

For eav:
```
create local temp table tmp_mongo_eav 
(
	id varchar(32), 
	keys varchar(8000), 
	values varchar(32000)
) 
order by id 
segmented by hash(id) all nodes;
```

```
COPY tmp_mongo_eav WITH SOURCE MongodbSource
(
	url='mongodb://host1:27017,host2:27017/?replicaSet=rs01',
	database='dwh',
	collection='clickstream',
	query='{"_id": {"$lte": {"$oid": "5cde76f7a90e7ba133edbae1"}, "$gt": {"$oid": "5cde74d3a90e7ba1339e73ff"}}}',
	format='eav',
	delimiter=E'\001',
	terminator=E'\002'
)
DELIMITER E'\001' RECORD TERMINATOR E'\002' NO ESCAPE DIRECT
REJECTMAX 0 ABORT ON ERROR;
```

For columns:
```
CREATE LOCAL TEMP TABLE tmp_mongo_array  
(
	_id_str varchar(36),
	eid int,
	src_id int,
	src int,
	dt int,
	dtm numeric(37,15),
	u varchar(64),
	uid int,
	ua varchar(1024),
	ip varchar(1024),
	url varchar(1024),
	keys varbinary(2000),
	values varchar(16000)
)
ON COMMIT PRESERVE ROWS
ORDER BY _id_str
SEGMENTED BY hash(_id_str) ALL NODES;
```

```
COPY tmp_mongo_array
(
	_id_str, keys_comp filler varchar(4000),
	eid, src_id, src, dt, dtm, u, uid, ua, ip, url, 
	keys as hex_to_binary(replace(keys_comp, '#', '00')), values
)
WITH SOURCE MongodbSource
(                                 
	url='mongodb://host1:27017,host2:27017/?replicaSet=rs01',
	database='dwh',
	collection='clickstream',
	query='{"_id": {"$lte": {"$oid": "5cde76f7a90e7ba133edbae1"}, "$gt": {"$oid": "5cde74d3a90e7ba1339e73ff"}}}',
	format='array:eid=eid,src_id=src_id,src=src,dt=dt,dtm=dtm,u=u,uid=uid,ua=ua,ip=ip,url=url,ref=values,v=values,ab=values,bot=values,geo=values,app=values,lid=values,cid=values,q=values,iid=values,mid=values',
	delimiter=E'\001',                 
	terminator=E'\002'
)                
DELIMITER E'\001' RECORD TERMINATOR E'\002' NO ESCAPE DIRECT
REJECTMAX 0 ABORT ON ERROR;
```
