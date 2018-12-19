#!/bin/bash

/usr/bin/g++-4.8 -I /opt/vertica/sdk/include -I. -Itsl/ -std=gnu++1y -Wall -shared -Wno-unused-value -Wno-narrowing -fPIC -O3 -o DistinctHash.so distinct_hash.cpp /opt/vertica/sdk/include/Vertica.cpp 

