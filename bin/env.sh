#!/bin/bash

JVM_ARGS=""
CLASSPATH="$CLASSPATH:${BIN_HOME}"
for jar in `ls ${JAFKA_HOME}/lib/*.jar`;do
    CLASSPATH=$CLASSPATH:$jar
done

#echo "APP_HOME: $APP_HOME"
#echo "CLASSPATH: $CLASSPATH"

export CLASSPATH JVM_ARGS
