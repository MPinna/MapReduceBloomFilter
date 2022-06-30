import re
import sys
import os
import util
from operator import add
from BloomFilter import BloomFilter
from pyspark import RDD, SparkContext, rdd
from util import *

NUM_OF_ARGS = 5
PROTOCOL = "hdfs://"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 9000
NUM_OF_RATINGS = 10

#TODO same exact function as the one in spark_bloomfilter_versions.py
# move it into Utils or leave as it is?
#LINK ./spark_bloomfilter_versions.py:12

def mapRatingMovie(line: str):
    line_ = line.split()
    rating = roundHalfUp(line_[1]) 
    movieId = line_[2]
    return (rating, movieId)

def FPR_map(line: str):
    bloom_filters_by_rating = {}
    false_positive_count = [0]*NUM_OF_RATINGS
    true_negative_count = [0]*NUM_OF_RATINGS

    #TODO likely not correct
    #FIXME
    with open(bloom_filters_file) as f:
        for line_ in f.readlines():
            temp_bloom_filter = BloomFilter(line_)
            bloom_filters_by_rating[temp_bloom_filter.m] = temp_bloom_filter

    tokens = mapRatingMovie(line)
    movie_rating = tokens[0]
    movie_id = tokens[1]
    curr_bloom_filter = bloom_filters_by_rating[0]
    for i in range(NUM_OF_RATINGS):
        curr_bloom_filter_rating = i+1
        curr_bloom_filter: BloomFilter = bloom_filters_by_rating[curr_bloom_filter_rating]
        # REVIEW is check on None correct?
        if(curr_bloom_filter == None):
            continue
        movie_in_filter = curr_bloom_filter.test(movie_id)
        if(movie_in_filter and movie_rating != curr_bloom_filter_rating):
            false_positive_count[i] += 1
        if(curr_bloom_filter_rating != movie_rating):
            true_negative_count[i] += 1
    return (false_positive_count, true_negative_count)
    
    
#REVIEW
def FPR_reduce(counts_a: tuple, counts_b: tuple):
    cumul_false_positives = list(map(add, counts_a[0], counts_b[0]))
    cumul_true_negatives = list(map(add, counts_a[1], counts_b[1]))
    return (cumul_false_positives, cumul_true_negatives)


if __name__== "__main__":

    argv_len = len(sys.argv)
    if(argv_len < NUM_OF_ARGS):
        print("Usage: > spark-submit spark_FPR_test.py <master> <input> <output> <partitions> <path_to_bloom_filters_file> [<defaultFS> = \"localhost\" [<defaultFSPort> = 9000]]", file=sys.stderr)
        exit(1)

    master = sys.argv[1]
    if(master not in MASTER_TYPES):
        print("Invalid master type. Select one from {0}".format(MASTER_TYPES),  file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="FPR_RATE", master= master, pyFiles=["util.py", "BloomFilter.py"])

    #TODO check if this is correct
    if master == "yarn":
        #Reuse python executable obtained from virtualenv
        os.environ['PYSPARK_PYTHON'] = "./environment/bin/python"

    input = sys.argv[2]
    output = sys.argv[3]
    partitions = sys.argv[4]
    path_to_bloom_filters_file = sys.argv[5]
    host = DEFAULT_HOST
    port = DEFAULT_PORT

    if(argv_len > NUM_OF_ARGS):
        fs = sys.argv[6]

    if(argv_len > NUM_OF_ARGS + 1):
        default_port = int(sys.argv[7])

    base_hdfs = PROTOCOL + fs + ":" + str(port)
    input_file_path = base_hdfs + input
    output_file_path = base_hdfs + output
    bloom_filters_file = base_hdfs + path_to_bloom_filters_file

    print(f"[LOG] input: {input_file_path}")
    print(f"[LOG] output: {output_file_path}")
    print(f"[LOG] partitions: {partitions}")
    print(f"[LOG] path_to_bloom_filters_file: {path_to_bloom_filters_file}")
    print(f"[LOG] base_hdfs: {base_hdfs}")

    rdd_input: RDD = sc.textFile(input_file_path, partitions)
    
    rows: RDD = rdd_input.filter(removeHeaderAndMalformedRows)

    rows_grouped: RDD = rows.map(mapRatingMovie).groupByKey()

    rows_mapped: RDD = rows_grouped.map(FPR_map)

    rows_reduced: RDD = rows_mapped.reduceByKey(FPR_reduce)

    rows_reduced_sorted: RDD = rows_reduced.sortByKey()

    output_rdd: RDD = rows_reduced_sorted.map(lambda x: str(x[0]/(x[0] + x[1])))

    output_rdd.saveAsTextFile(output_file_path)
    


    