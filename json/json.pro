#-------------------------------------------------
#
# Project created by QtCreator 2017-10-02T18:27:27
#
#-------------------------------------------------

QT       -= core gui

TARGET = json
TEMPLATE = lib
CONFIG += c++11
DEFINES += RAPID_JSON_LIBRARY 
SOURCES += json.cpp
INCLUDEPATH += /opt/vertica/sdk/include/

unix {
    target.path = /usr/lib
    INSTALLS += target
}
