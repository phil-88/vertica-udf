#!/bin/bash

g++-4.8 -I /opt/vertica/sdk/include -I. -Itsl/ -std=gnu++1y -Wall -shared -Wno-unused-value -Wno-narrowing -fPIC -O3 -o Transpose.so transpose.cpp /opt/vertica/sdk/include/Vertica.cpp 

