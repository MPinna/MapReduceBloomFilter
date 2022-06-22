#!/bin/bash
mkdir -p output
mkdir -p logs

# Number of repetitions
N_BLOOM_TEST=10

INPUT="title.ratings.tsv"
JAR_FILE=~/it/unipi/hadoop/BloomFilter/1.0/BloomFilter-1.0.jar

# Configurations
P=(0.00001 0.0001 0.001 0.01 0.1)   ## 0.001%, 0.01%, 0.1%, 1%, 10%
K=(0 3 5 7 9)                     ## K=0 means no constraints    

MAPPERS=(4 6 8 10 12)
MAP_P=0.001
MAP_K=0

function now(){
  TZ=":Europe/Rome" date +"%d-%m-%Y_%H.%M.%S"
}

function launch_test(){
  i=0
  while [ $i -lt $N_BLOOM_TEST ]
  do
    echo -en "{\"name\": \"$3P$1K$2$4_$i\","
    OUT_FILE="bloom$3P$1K$2$4_$i"
    INPUT="input/P$1K$2$4"

    start=$(date +"%s%N")
    hadoop jar $JAR_FILE it.unipi.hadoop.MapRedBloomFilter $(bash $INPUT $OUT_FILE $3)  &> .tmp
    stop=$(date +"%s%N")

    echo -e "\"wallTime\":$(($stop-$start)),"
    echo -n "\"log\":\""
    cat .tmp | tr '\t' ' ' | tr '"' "'" | awk '{printf "%s\\n", $0}'

  if [[ $i -eq $N_BLOOM_TEST-1 ]]
  then 
    echo -e "\"}"
  else
    echo -e "\"},"
  fi

  i=$(( $i + 1 ))

  done
}

function launch_all(){
  echo -e "{\"start\":\"$(now)\",\n\"tests\":{"

  echo -e "\"WithBloomFilters\":{"
  for p in ${P[@]}; do
    for k in ${K[@]}; do 
      echo -e "\"P$p""K$k\":["
      launch_test $p $k WithBloomFilters
      echo -e "],"
    done
  done
  for m in ${MAPPERS[@]}; do
    echo -e "\"P$P""K$K""MAP$m\":["
    launch_test $MAP_P $MAP_K WithBloomFilters MAP$m
    if (( $(echo "$m == ${MAPPERS[-1]}" | bc -l) ))
    then
      echo -e "]"
    else
      echo -e "],"
    fi
  done
  echo -e "},"
  
  
  echo -e "\"WithIndexes\":{"
    for p in ${P[@]}; do
      for k in ${K[@]}; do 
        echo -e "\"P$p""K$k\":["
        launch_test $p $k WithIndexes
        echo -e "],"
    done
  done
  for m in ${MAPPERS[@]}; do
  echo -e "\"P$P""K$K""MAP$m\":["
  launch_test $MAP_P $MAP_K WithIndexes MAP$m
  if (( $(echo "$m == ${MAPPERS[-1]}" | bc -l) ))
  then
    echo -e "]"
  else
    echo -e "],"
  fi
  done
  echo -e "}},"
  
  rm .tmp

  echo -e "\"stop\":\"$(now)\"}"
}

### START TEST ###

launch_all 2>&1 | tee logs/test_log_$(now).json

# echo "Getting results ..."
# hadoop fs -get bloom* output