#-------------------------------------------------
#
# Project created by QtCreator 2017-10-02T18:27:27
#
#-------------------------------------------------

QT       -= core gui

TARGET = distinct_hash
TEMPLATE = lib
CONFIG += c++11
DEFINES += TRAFFIC_LIBRARY
SOURCES += distinct_hash.cpp
INCLUDEPATH += /opt/vertica/sdk/include/

unix {
    target.path = /usr/lib
    INSTALLS += target
}
