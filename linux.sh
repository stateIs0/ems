#!/bin/bash
date

mvn clean package  -DskipTests

cd example-parent/dispatcher

mvn clean package assembly:single -DskipTests

cd ../..

nohup java -jar  example-parent/dispatcher/target/dispatcher-1.0.0-SNAPSHOT-jar-with-dependencies.jar &