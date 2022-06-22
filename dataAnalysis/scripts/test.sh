#!/bin/bash
mkdir -p output
mkdir -p logs

# Number of repetitions
N_BLOOM_TEST=10

INPUT="title.ratings.tsv"
JAR_FILE=~/it/unipi/hadoop/BloomFilter/1.0/BloomFilter-1.0.jar

# Configurations
P=(0.00001 0.0001 0.001 0.01 0.1)   ## 0.001%, 0.01%, 0.1%, 1%, 10%
K=(0 1 3 5 7 9)                     ## K=0 means no constraints    

MAPPERS=(4 6 8 10 12)

function now(){
  TZ=":Europe/Rome" date +"%d-%m-%Y_%H.%M.%S"
}

function launch_test(){
  i=0
  while [ $i -lt $N_BLOOM_TEST ]
  do
    echo -en "{\"name\": \"$3P$1K$2_$i\","
    OUT_FILE="bloom$3P$1K$2_$i"
    INPUT="input/P$1K$2"

    start=$(date +"%s%N")
    hadoop jar $JAR_FILE it.unipi.hadoop.MapRedBloomFilter $(bash $INPUT $OUT_FILE $3)  &> .tmp
    stop=$(date +"%s%N")

    echo -e "\"wallTime\":$(($stop-$start)),"
    echo -n "\"log\":\""
    cat .tmp | tr '\t' ' ' | awk '{printf "%s\\n", $0}'

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
      if (( $(echo "$p == ${P[-1]}" | bc -l) && $(echo "$k == ${K[-1]}" | bc -l) ))
      then
        echo -e "]"
      else
        echo -e "],"
      fi
    done
  done
  ## TODO: changing mappers
  echo -e "},"
  
  
  echo -e "\"WithIndexes\":{"
    for p in ${P[@]}; do
      for k in ${K[@]}; do 
        echo -e "\"P$p""K$k\":["
        launch_test $p $k WithIndexes
        if (( $(echo "$p == ${P[-1]}" | bc -l) && $(echo "$k == ${K[-1]}" | bc -l) ))
        then
          echo -e "]"
        else
          echo -e "],"
        fi
    done
  done
  ## TODO: changing mappers
  echo -e "}},"
  
  rm .tmp

  echo -e "\"stop\":\"$(now)\"}"
}

### START TEST ###

launch_all 2>&1 | tee logs/test_log_$(now).json

# echo "Getting results ..."
# hadoop fs -get bloom* output