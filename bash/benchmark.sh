#!/bin/bash
PG_HOST=postgres
TIMEFORMAT=%R
#METHODS=( "naive" "breadth-first" "depth-first")
METHODS=( "breadth-first" "depth-first")

# single-threaded
echo "EFO : Single Thread versions"

for method in "${METHODS[@]}";
do
  echo "Running ${method} (ST)"
  python -m efo_etl -d ${PG_HOST} --bulk ${method}
  echo "Done ${method} (ST)"
done

# multithreaded
WORKER_COUNTS=("4" "8" "16")
echo "EFO : Multi-Thread versions"
for worker_count in "${WORKER_COUNTS[@]}";
do
  for method in "${METHODS[@]}";
  do
    echo "Running ${method} (MT=${worker_count})"
    python -m efo_etl -d ${PG_HOST} -w ${worker_count} --bulk ${method}
    echo "Done ${method} (MT=${worker_count})"
  done
done
