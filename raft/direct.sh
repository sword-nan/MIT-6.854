#!/bin/bash
runs=100
if [ $# -gt 0 ]; then
	runs="$1"
fi

parallelism=$(grep -c processor /proc/cpuinfo)
if [ $# -gt 1 ]; then
	parallelism="$2"
fi

test=""
if [ $# -gt 2 ]; then
	test="$3"
fi

output="output.txt"

bash go-test-many.sh "$runs" "$parallelism" "$test" | sed 's/\x1b\[[0-9;]*m//g' >"$output"
