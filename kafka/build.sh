#!/bin/bash

if [ ! -e /usr/local/include/cppkafka ] ; then
	sh prereq.sh
fi

g++ $CXXFLAGS -fPIC -std=gnu++1y \
    -I /opt/vertica/sdk/include -I. -Itsl/ \
    -I /usr/local/include/ \
    -O3 -march=native -mtune=native \
    -Wall -Wno-unused-value -Wno-narrowing \
    -shared -o KafkaConnector.so kafka_connector.cpp \
    /opt/vertica/sdk/include/Vertica.cpp \
    /usr/local/lib/libcppkafka.a /usr/local/lib/librdkafka.a \
    -lpthread -lrt -ldl -lz -lssl -lcrypto -lsasl2
