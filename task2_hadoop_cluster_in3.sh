#!/bin/sh

export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export CLASSPATH=`hadoop classpath`

echo --- Deleting
rm Task2.jar
rm Task2*.class

echo --- Compiling
$JAVA_HOME/bin/javac Task2.java
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf Task2.jar Task2*.class

echo --- Running
INPUT=/a2_inputs/in3.txt
OUTPUT=/user/${USER}/a2_starter_code_output/

hdfs dfs -rm -R $OUTPUT
time yarn jar Task2.jar Task2 $INPUT $OUTPUT

hdfs dfs 
