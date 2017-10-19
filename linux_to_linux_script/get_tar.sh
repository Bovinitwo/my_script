#!/bin/bash

basepath=$(cd $(dirname ${BASH_SOURCE}); pwd)

file_raw_path=${basepath}/../map_raw_input
file_done_path=${basepath}/../map_done_input

while [ -d ${file_raw_path} ]
do
    ls ${file_raw_path} | while read line
    do
        echo ${line}
		mv ${file_raw_path}/${line} ${file_done_path}       
	done
	sleep 10
done
