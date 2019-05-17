#!/bin/bash

WORKDIR=/tmp/mongo_driver
ARCLOCATION=http://aptly.msk.avito.ru/vertica/3d_party

mkdir -p $WORKDIR

export LD_LIBRARY_PATH=/usr/local/lib
export CXXFLAGS="$CXXFLAGS -fPIC"
export CFLAGS="$CFLAGS -fPIC"

cd $WORKDIR
wget $ARCLOCATION/snappy-1.1.3.tar.gz
tar -xzf snappy-1.1.3.tar.gz
cd $WORKDIR/snappy-1.1.3
./configure && make && make install

cd $WORKDIR
wget $ARCLOCATION/mongo-c-driver-1.14.0.tar.gz
tar -xzf mongo-c-driver-1.14.0.tar.gz
cd $WORKDIR/mongo-c-driver-1.14.0
mkdir cmake-build
cd cmake-build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_FLAGS="$CFLAGS" -DCMAKE_CXX_FLAGS="$CXXFLAGS" -DENABLE_SSL=OFF -DENABLE_SASL=OFF .. && make && make install

cd $WORKDIR
wget $ARCLOCATION/mnmlstc-core.tar.gz
tar -xzf mnmlstc-core.tar.gz
cd $WORKDIR/mnmlstc-core
mkdir cmake-build
cd cmake-build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_FLAGS="$CFLAGS" -DCMAKE_CXX_FLAGS="$CXXFLAGS" -DBUILD_TESTING=Off -DINCLUDE_INSTALL_DIR=/usr/include .. && make && make install

cd $WORKDIR
wget $ARCLOCATION/mongo-cxx-driver-r3.4.0.tar.gz
tar -xzf mongo-cxx-driver-r3.4.0.tar.gz
cd $WORKDIR/mongo-cxx-driver-r3.4.0
mkdir cmake-build
cd cmake-build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_FLAGS="$CFLAGS" -DCMAKE_CXX_FLAGS="$CXXFLAGS" -DCMAKE_INSTALL_PREFIX=/usr/local -DBUILD_SHARED_LIBS=OFF -DBSONCXX_POLY_USE_MNMLSTC=1 -DBSONCXX_POLY_USE_SYSTEM_MNMLSTC=1 -DMONGOCXX_ENABLE_SSL=OFF .. 
make -j4 && make install

