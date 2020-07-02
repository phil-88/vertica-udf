#!/bin/bash

ARCLOCATION=https://github.com/edenhill/librdkafka/archive/v1.1.0.tar.gz
wget -O - $ARCLOCATION | tar xz -C /tmp/
cd /tmp/librdkafka-1.1.0
mkdir cmake-build
cd cmake-build
cmake -DCMAKE_C_FLAGS="-D__STDC_VERSION__=201112 -fPIC" -DCMAKE_CXX_FLAGS="$CXXFLAGS -std=gnu++1y -D__STDC_VERSION__=201112 -fPIC" -DWITH_ZSTD=OFF -DENABLE_LZ4_EXT=OFF -DRDKAFKA_BUILD_STATIC=ON -DRDKAFKA_BUILD_EXAMPLES=OFF -DRDKAFKA_BUILD_TESTS=OFF .. \
 && make \
 && make install

ARCLOCATION=https://github.com/mfontanini/cppkafka/archive/master.tar.gz
wget -O - $ARCLOCATION | tar xz -C /tmp/
cd /tmp/cppkafka-master
mkdir cmake-build
cd cmake-build
cmake -DCPPKAFKA_BUILD_SHARED=OFF -DCPPKAFKA_DISABLE_EXAMPLES=ON -DCMAKE_CXX_FLAGS="$CXXFLAGS -fPIC -std=c++11" .. \
 && make \
 && make install
