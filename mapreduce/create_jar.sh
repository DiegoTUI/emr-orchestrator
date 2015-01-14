#!/bin/bash
 javac -classpath ./jar/hadoop-common-2.4.0.jar:./jar/hadoop-mapreduce-client-core-2.4.0.jar:./jar/hadoop-annotations-2.4.0.jar MapReduce.java -Xlint:deprecation
 jar cf mr.jar MapReduce*.class