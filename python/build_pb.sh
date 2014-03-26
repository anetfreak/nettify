#!/bin/bash
#
# creates the python classes for our .proto
#

project_base="/Users/Ameya/Documents/Ameya/MS/CMPE275-Gash/nettify"

rm ${project_base}python/src/comm_pb2.py

protoc -I=${project_base}/resources --python_out=./src ${project_base}/resources/comm.proto 
