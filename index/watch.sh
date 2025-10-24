#!/bin/bash

./build/index ../workload_traces/workloadc_large_load ../workload_traces/workloadc_large_run skip-list 1 64B 64B &

TARGET_PID=$!

echo "timestamp vsz rss time" > test.log

# Loop until the process exits
while kill -0 "$TARGET_PID" 2>/dev/null; do
  NOW=$(date +"%s.%3N")
  # ps_output=$(ps -p "$TARGET_PID" -o user=,pid=,pcpu=,pmem=,vsz=,rss=,time=)
  ps_output=$(ps -p "$TARGET_PID" -o vsz=,rss=,time=)
  
  echo "$NOW $ps_output" >> test.log
  
  sleep 0.1
done