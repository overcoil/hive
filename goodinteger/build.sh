#!/bin/sh
mvn clean
mvn package 

cp target/*.jar $HIVE_HOME/lib
