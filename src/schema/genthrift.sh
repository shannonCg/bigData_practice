#!/bin/bash
rm -rf ../main/java/com/shaice/bigdata/superwebanalytics/schema
thrift -r --gen java:beans,nocamel ./shema.thrift
cp ./gen-javabean/com/shaice/bigdata/superwebanalytics/schema ../main/java/com/shaice/bigdata/superwebanalytics
rm -rf ./gen-javabean