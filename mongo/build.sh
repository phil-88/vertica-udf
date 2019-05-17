#!/bin/bash

if [ ! -e /usr/local/include/mongocxx ] ; then 
	sh prereq.sh
fi

g++ $CXXFLAGS -fPIC -std=gnu++1y \
    -I /opt/vertica/sdk/include -I. -Itsl/ \
    -O3 -march=native -mtune=native \
    -Wall -Wno-unused-value -Wno-narrowing \
    -shared -o MongoConnector.so mongo_connector.cpp /opt/vertica/sdk/include/Vertica.cpp \
    -DMONGOCXX_STATIC -DBSONCXX_STATIC -DMONGOC_STATIC -DBSON_STATIC \
    -I/usr/local/include/libmongoc-1.0 \
    -I/usr/local/include/libbson-1.0 \
    -I/usr/local/include/mongocxx/v_noabi \
    -I/usr/local/include/bsoncxx/v_noabi \
    -L/usr/local/lib -lmongocxx-static -lbsoncxx-static \
    -lmongoc-static-1.0 -lbson-static-1.0 \
    -lm -lpthread -lresolv -lrt -lz \
    -Wl,-Bstatic -lsnappy -Wl,-Bdynamic #\
    #-lsasl2 -lssl -lcrypto 

