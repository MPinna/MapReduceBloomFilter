#!/bin/bash
mkdir -p test/output

# Number of repetitions
N_PARAMS_TEST=1
N_BLOOM_TEST=1
N_RATE_TEST=1


# Configurations
M=(1024, 2048, 4096, 8192)
K=(1,2,3)
N_SPLIT=(100000, 200000)
P=(0.1 0.01 0.001)


function now(){
  TZ=":Europe/Rome" date +"%d-%m-%Y_%H.%M.%S"
}

function launch_compute_params(){
  i=0
  while [ $i -lt $N_PARAMS_TEST ]
  do
    echo -en "{\"name\": \"param_$1_$i\","
    OUT_FILE="params_$1_$i"
    start=$(date +"%s%N")
    hadoop jar BloomFilter-1.0.jar it.unipi.hadoop.MapRedComputeParams $(bash test/input/params.in $OUT_FILE $1) &> .tmp    
    stop=$(date +"%s%N")
    echo -e "\"wallTime\":$(($stop-$start)),"
    echo -n "\"log\":\""
    cat .tmp | tr '\t' ' ' | awk '{printf "%s\\n", $0}'

    if [[ $i -eq $N_PARAMS_TEST-1 ]]
    then 
      echo -e "\"}"
    else
      echo -e "\"},"
    fi
    i=$(( $i + 1 ))
  done
}

function launch_bloom(){
  i=0
  while [ $i -lt $N_BLOOM_TEST ]
  do
    echo -en "{\"name\": \"bloom$1_$i\","
    OUT_FILE="bloom$1_$i"
    start=$(date +"%s%N")
    hadoop jar BloomFilter-1.0.jar it.unipi.hadoop.MapRedBloomFilter $(bash test/input/bloom.in $OUT_FILE $1)  &> .tmp
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

function launch_rate(){
  i=0
  while [ $i -lt $N_RATE_TEST ]
  do
    echo -en "{\"name\": \"rate_$i\","
    OUT_FILE="rate_$i"
    start=$(date +"%s%N")
    #hadoop jar BloomFilter-1.0.jar it.unipi.hadoop. $(bash test/input/rate.in $OUT_FILE)  &> .tmp
    stop=$(date +"%s%N")
    echo -e "\"wallTime\":$(($stop-$start)),"
    echo -n "\"log\":\""
    #cat .tmp | tr '\t' ' ' | awk '{printf "%s\\n", $0}'
    
    if [[ $i -eq $N_RATE_TEST-1 ]]
    then 
      echo -e "\"}"
    else
      echo -e "\"},"
    fi
  
    i=$(( $i + 1 ))
  done
}

function launch_test(){
  echo -e "{\"start\":\"$(now)\",\n\"tests\":{"

  echo -e "\"params\":{"
    for p in ${P[@]}
    do 
      echo -e "\"P$p\":["
      launch_compute_params $p
      if (( $(echo "$p == ${P[-1]}" | bc -l) ))
      then
        echo -e "]"
      else
        echo -e "],"
      fi
    done
  echo -e "},"

  echo -e "\"bloom\":["
    launch_bloom WithBloomFilters
  echo -e "],"


  echo -e "\"bloomIndexes\":["
    launch_bloom WithIndexes
  echo -e "],"


  echo -e "\"rate\":["
    launch_rate
  echo -e "]},"

  rm .tmp

  echo -e "\"stop\":\"$(now)\"}"
}

### START TEST ###

launch_test 2>&1 | tee test/test_log_$(now).json

echo "Getting results ..."

hadoop fs -get params* test/output 
hadoop fs -get bloom* test/output
hadoop fs -get rate* test/output