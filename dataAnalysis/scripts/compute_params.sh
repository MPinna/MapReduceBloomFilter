#!/bin/bash
mkdir -p params
mkdir -p logs

INPUT="title.ratings.tsv"
JAR_FILE=~/it/unipi/hadoop/BloomFilter/1.0/BloomFilter-1.0.jar

# Configurations
P=(0.00001 0.0001 0.001 0.01 0.1)     ## 0.001%, 0.01%, 0.1%, 1%, 10%
K=(0 3 5 7 9)                         ## K=0 means no constraints    

function now(){
  TZ=":Europe/Rome" date +"%d-%m-%Y_%H.%M.%S"
}

function launch_compute_params(){
    echo -en "{\"name\": \"params_P$1_K$2\","
    OUT_FILE="params_P$1_K$2"
    start=$(date +"%s%N")
    hadoop jar $JAR_FILE it.unipi.hadoop.MapRedComputeParams $INPUT $OUT_FILE $1 157000 $2 &> .tmp    
    stop=$(date +"%s%N")
    echo -e "\"wallTime\":$(($stop-$start)),"
    echo -n "\"log\":\""
    cat .tmp | tr '\t' ' ' | awk '{printf "%s\\n", $0}'
    echo -e "\"}"
}

function launch_all(){
  echo -e "{\"start\":\"$(now)\",\n\"tests\":{"

  echo -e "\"params_\":{"
    for p in ${P[@]}; do
        for k in ${K[@]}; do 
        echo -en "\"P$p"
        echo -e "K$k\":["
        launch_compute_params $p $k
        if (( $(echo "$p == ${P[-1]}" | bc -l) && $(echo "$k == ${K[-1]}" | bc -l) ))
        then
          echo -e "]"
        else
          echo -e "],"
        fi
      done
    done
  echo -e "}},"

  rm .tmp

  echo -e "\"stop\":\"$(now)\"}"
}

### START COMPUTATIONS ###

launch_all 2>&1 | tee logs/params_log_$(now).json

echo "Getting results ..."

hadoop fs -getmerge params_* params/params.txt 
hadoop fs -get params_* params/ 
