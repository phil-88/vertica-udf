#!/bin/bash

g++ $CXXFLAGS -I /opt/vertica/sdk/include -I. -Itsl/ -std=gnu++1y -Wall -shared -Wno-unused-value -Wno-narrowing -fPIC -O3 -o RapidJson.so json.cpp /opt/vertica/sdk/include/Vertica.cpp 

g++ $CXXFLAGS -I /opt/vertica/sdk/include -I. -Itsl/ -std=gnu++1y -Wall -shared -Wno-unused-value -Wno-narrowing -fPIC -O3 -o RapidArray.so array.cpp /opt/vertica/sdk/include/Vertica.cpp

