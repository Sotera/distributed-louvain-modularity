#! /bin/bash

echo "clear out old example dir.."
hadoop fs -rm -r /tmp/louvain-giraph-example

echo "loading example data to /tmp/louvain-giraph-example"
hadoop fs -mkdir /tmp/louvain-giraph-example
hadoop fs -mkdir /tmp/louvain-giraph-example/input
hadoop fs -mkdir /tmp/louvain-giraph-example/output
hadoop fs -put small.tsv /tmp/louvain-giraph-example/input/small.tsv

echo "running the full louvain pipeline"
cd ..
python louvain.py /tmp/louvain-giraph-example/input /tmp/louvain-giraph-example/output


