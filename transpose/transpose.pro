#-------------------------------------------------
#
# Project created by QtCreator 2017-10-02T18:27:27
#
#-------------------------------------------------

QT       -= core gui

TARGET = transpose
TEMPLATE = lib
CONFIG += c++11
DEFINES += TRANSPOSE_LIBRARY
SOURCES += transpose.cpp
INCLUDEPATH += /opt/vertica/sdk/include/

unix {
    target.path = /usr/lib
    INSTALLS += target
}
