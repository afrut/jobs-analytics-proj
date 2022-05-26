#!/bin/bash
export PATH=$PATH":/mnt/d/kafka_2.13-3.1.0/bin"
export KAFKA_HOME="/mnt/d/kafka_2.13-3.1.0"
IFS=':' read -r -a array <<< $PATH
for element in "${array[@]}"
do
    echo "$element"
done