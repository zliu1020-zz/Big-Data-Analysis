#!/bin/sh

export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export CLASSPATH=`hadoop classpath`

echo --- Deleting
rm Task4.jar
rm Task4*.class

echo --- Compiling
$JAVA_HOME/bin/javac -Xlint:deprecation Task4.java
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf Task4.jar Task4*.class

echo --- Running
INPUT=/a2_inputs/in3.txt
OUTPUT=/user/${USER}/a2_starter_code_output/

hdfs dfs -rm -R $OUTPUT
time yarn jar Task4.jar Task4 $INPUT $OUTPUT

hdfs dfs 
