#!/bin/bash
rm -rf ../main/java/com/shaice/bigdata/superwebanalytics/schema
thrift -r --gen java:beans,nocamel ./schema.thrift
mv ./gen-javabean/com/shaice/bigdata/superwebanalytics/schema ../main/java/com/shaice/bigdata/superwebanalytics
rm -rf ./gen-javabean
