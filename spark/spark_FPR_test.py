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
NUM_OF_RATINGS = 10

#TODO same exact function as the one in spark_bloomfilter_versions.py
# move it into Utils or leave as it is?
#LINK ./spark_bloomfilter_versions.py:12

def mapRatingMovie(line: str):
    line_ = line.split()
    rating = roundHalfUp(line_[1]) 
    movieId = line_[0]
    return (rating, movieId)

def FPR_map(item: tuple):
    bloom_filters_by_rating = {}
    false_positive_count = [0]*NUM_OF_RATINGS
    true_negative_count = [0]*NUM_OF_RATINGS

    # initialize collection of bloom filters
    for line in bloom_filters_list_br.value:
        temp_bloom_filter: BloomFilter = BloomFilter(json_string=line.strip())
        bloom_filters_by_rating[temp_bloom_filter.rating] = temp_bloom_filter

    movie_rating = item[0]
    movie_ids = item[1]

    for i in range(NUM_OF_RATINGS):
        curr_bloom_filter_rating = i+1
        curr_bloom_filter: BloomFilter = bloom_filters_by_rating.get(curr_bloom_filter_rating)
        if(curr_bloom_filter == None):
            continue
        for movie_id in movie_ids:
            movie_in_filter = curr_bloom_filter.test(movie_id)
            if(movie_in_filter and movie_rating != curr_bloom_filter_rating):
                false_positive_count[i] += 1
            if(curr_bloom_filter_rating != movie_rating):
                true_negative_count[i] += 1


    return [(i + 1, (false_positive_count[i], true_negative_count[i])) for i in range(NUM_OF_RATINGS)]

    
def FPR_reduce(counts_a: tuple, counts_b: tuple):
    cumul_false_positives = counts_a[0] + counts_b[0]
    cumul_true_negatives = counts_a[1] + counts_b[1]
    return (cumul_false_positives, cumul_true_negatives)

def get_rate_from_counts(x: tuple):
    rating = x[0]

    FP = x[1][0]    
    TN = x[1][1]

    FPR = FP/(FP + TN)
    return (rating, FPR)



if __name__== "__main__":

    argv_len = len(sys.argv)
    if(argv_len < NUM_OF_ARGS):
        print("Usage: > spark-submit spark_FPR_test.py <master> <host> <port> <input> <output> <path_to_bloom_filters_file>", file=sys.stderr)
        exit(1)

    master = sys.argv[1]
    if not re.search(util.MASTER_TYPES_REGEX,master):
        print(f"Invalid master type. Select one from {util.MASTER_TYPES}",  file=sys.stderr)
        sys.exit(-1)


    if master == "yarn":
        #Reuse python executable obtained from virtualenv
        os.environ['PYSPARK_PYTHON'] = "./environment/bin/python"

    sc = SparkContext(appName="FPR_RATE", master= master, pyFiles=["util.py", "BloomFilter.py"])
    host = sys.argv[2]
    port = sys.argv[3]
    base_hdfs = "hdfs://" + host + ":" + port + "/user/hadoop/"
    input_file_path = base_hdfs + sys.argv[4]
    output_file_path = base_hdfs + sys.argv[5]
    bloom_filters_file = base_hdfs + sys.argv[6]

    print(f"[LOG] input: {input_file_path}")
    print(f"[LOG] output: {output_file_path}")
    print(f"[LOG] bloom_filters_file: {bloom_filters_file}")
    print(f"[LOG] base_hdfs: {base_hdfs}")

    bloom_filters_rdd = sc.textFile(bloom_filters_file)
    bloom_filters_list = bloom_filters_rdd.collect()
    bloom_filters_list_br = sc.broadcast(bloom_filters_list)

    # TODO remove test stuff
    # print(f"[LOG] *************************************\n"*20)
    # bloom_filters_by_rating = {}    
    # for line in bloom_filters_list_br.value:
    #     temp_bloom_filter: BloomFilter = BloomFilter(line.strip())
    #     print(f"[LOG] temp bloom filter: {temp_bloom_filter}")
    #     bloom_filters_by_rating[temp_bloom_filter.rating] = temp_bloom_filter
        
    rdd_input: RDD = sc.textFile(input_file_path)
    
    rows: RDD = rdd_input.filter(removeHeaderAndMalformedRows) 
    
    rows_grouped: RDD = rows.map(mapRatingMovie).groupByKey()
        
    rows_mapped: RDD = rows_grouped.flatMap(FPR_map)


    print(f"[LOG] *************************************\n"*20)
    print(f"[LOG] rows_mapped: {rows_mapped.collect()}")
    rows_reduced: RDD = rows_mapped.reduceByKey(FPR_reduce)
    
    
    print(f"[LOG] *************************************\n"*20)
    print(f"[LOG] rows_reduced: {rows_reduced.collect()}")
    
    output_rdd: RDD = rows_reduced.map(get_rate_from_counts)


    print(f"[LOG] *************************************\n"*20)
    print(f"[LOG] output_rdd: {output_rdd.collect()}")
    
    output_rdd.saveAsTextFile(output_file_path)