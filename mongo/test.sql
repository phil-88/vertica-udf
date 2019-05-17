
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


\a
\t

\o /tmp/mongodb.csv
select '''clickstream_' || to_char(getdate() - interval '4 hours', 'YYYY-MM-DD_HH') || '-00''';
\set mongodb `cat /tmp/mongodb.csv`

\o /tmp/mongoquery.csv
select '''{"_id": {"$lte": {"$oid": "' || 
	to_hex(extract(epoch from getdate() - interval '4 hours 1 minute')::int) || REPEAT('00',8) ||
	'"}, "$gt": {"$oid": "' ||
	to_hex(extract(epoch from getdate() - interval '4 hours 2 minute')::int) || REPEAT('00',8) ||
	'"}}}''';
\set mongoquery `cat /tmp/mongoquery.csv`

\a
\t
\o | cat


COPY tmp_mongo_array
(
	_id_str, keys_comp filler varchar(4000),
	eid, src_id, src, dt, dtm, u, uid, ua, ip, url, 
	keys as hex_to_binary(replace(keys_comp, '#', '00')), values
)
WITH SOURCE MongodbSource
(                                 
	url='mongodb://avi-memgo31.msk.avito.ru:27017,avi-memgo32.msk.avito.ru:27017/?replicaSet=rs26',                              
	database=:mongodb,                              
	collection='clickstream',                             
	query=:mongoquery,
	format='array:eid=eid,src_id=src_id,src=src,dt=dt,dtm=dtm,u=u,uid=uid,ua=ua,ip=ip,url=url,ref=values,v=values,ab=values,bot=values,geo=values,app=values,lid=values,cid=values,q=values,iid=values,mid=values,i=values,iids=values',
	delimiter=E'\001',                 
	terminator=E'\002'
)                
DELIMITER E'\001' RECORD TERMINATOR E'\002' NO ESCAPE DIRECT
REJECTMAX 0 ABORT ON ERROR;


