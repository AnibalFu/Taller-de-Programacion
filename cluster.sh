#!/bin/bash

ports=($(seq 8088 8096))

for i in {0..8}; do
    NODE_ID=$((i + 1))
    NODE_PORT=${ports[$i]}
    echo $NODE_ID
    echo $NODE_PORT
    CLUSTER_PORT=$((10000 + NODE_PORT))

    ./node.sh $NODE_ID $NODE_PORT $CLUSTER_PORT
done
