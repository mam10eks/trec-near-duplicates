#!/bin/bash -e

PARALLELISM=350
OUTPUT_FILE="ceph-parseable-files.jsonl"

if [ -z "$IA_S3_ACCESS_KEY" ]
then
	echo "Please set an environment variable IA_S3_ACCESS_KEY that I can continue."
	exit 1
fi

if [ -z "$IA_S3_SECRET_KEY" ]
then
	echo "Please set an environment variable IA_S3_SECRET_KEY that I can continue."
	exit 1
fi

make install
hdfs dfs -rm -r -f ${OUTPUT_FILE}

spark-submit \
	--class de.webis.trec_ndd.ceph_playground.ReportParseableFiles \
	--deploy-mode cluster \
	--conf spark.default.parallelism=${PARALLELISM}\
	--num-executors ${PARALLELISM}\
	--executor-memory 15G\
	--driver-memory 15G\
	target/trec-ndd-1.0-SNAPSHOT-jar-with-dependencies.jar \
	--bucketName maiks-test-2 \
	--accessKey ${IA_S3_ACCESS_KEY} \
	--secretKey ${IA_S3_SECRET_KEY} \
	--outputFile ${OUTPUT_FILE}

