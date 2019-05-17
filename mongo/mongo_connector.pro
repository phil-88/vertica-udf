#-------------------------------------------------
#
# Project created by QtCreator 2017-10-02T18:27:27
#
#-------------------------------------------------

QT       -= core gui

TARGET = mongo_connector
TEMPLATE = lib
CONFIG += c++11
QMAKE_CXXFLAGS_RELEASE = -Wno-unused-parameter -fPIC
QMAKE_CFLAGS_RELEASE = -Wno-unused-parameter -fPIC
DEFINES += MONGOCXX_STATIC BSONCXX_STATIC MONGOC_STATIC BSON_STATIC
SOURCES += mongo_connector.cpp
INCLUDEPATH += /opt/vertica/sdk/include/ \
	/usr/local/include/libmongoc-1.0 \
	/usr/local/include/libbson-1.0 \
	/usr/local/include/mongocxx/v_noabi \
	/usr/local/include/bsoncxx/v_noabi

LIBS += -L/usr/local/lib -lmongocxx-static -lbsoncxx-static -lmongoc-static-1.0 -lbson-static-1.0 \
	-lsasl2 -lssl -lcrypto -lm -lpthread -lresolv -lrt -lz \
        -Wl,-Bstatic -lsnappy -Wl,-Bdynamic

unix {
    target.path = /usr/lib
    INSTALLS += target
}

