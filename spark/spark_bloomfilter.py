import re
import sys
import util
import os
from BloomFilter import BloomFilter
from pyspark import SparkContext, rdd, SparkConf
from util import *


def mapRatingMovie(line: str):
    line_ = line.split()
    rating = roundHalfUp(line_[1]) 
    movieId = line_[2]
    return (rating, movieId)

def mapBF(item):    
    rating = item[0]
    movieIds = item[1]
    bloomFilter = BloomFilter(rating, m_list_br.value[rating - 1], k_br.value, p_br.value)
    
    for movieId in movieIds: 
        bloomFilter.add(movieId)

    return (rating, bloomFilter)

def reduce_bloomfilters(bloomfilter_a: BloomFilter, bloomfilter_b: BloomFilter):
    return bloomfilter_a.orBloomFilter(bloomfilter_b)

if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) != 18:
        print("Usage: > spark-submit spark_bloomfilter.py <master> <input_file> <output_file> <m>[10 times] <k> <p> <host> <port>", file=sys.stderr)
        sys.exit(-1)
    
    #Parse cmd line input
    master = sys.argv[1]
    if not re.search(util.MASTER_TYPES_REGEX,master):
        print("Invalid master type. Select one from {0}".format(MASTER_TYPES),  file=sys.stderr)
        sys.exit(-1)
        
    if master == "yarn":
        #Reuse python executable obtained from virtualenv
        os.environ['PYSPARK_PYTHON'] = './environment/bin/python'
        
    # Include all python files in pyFiles argument (python packages need to be installed via virtualenv)
    sc = SparkContext(appName="BLOOM_FILTER", master=master, pyFiles=["util.py", "BloomFilter.py"])

    host = sys.argv[16]
    port = sys.argv[17]
    base_hdfs = "hdfs://" + host + ":" + port + "/"
    input_hdfs_path = base_hdfs + sys.argv[2]
    output_hdfs_path = base_hdfs + sys.argv[3]
    
    try:
        m_list = [int(m) for m in sys.argv[4:14]]
        m_list_br = sc.broadcast(m_list)
        k = int(sys.argv[14])
        k_br = sc.broadcast(k)
        p = float(sys.argv[15])
        p_br = sc.broadcast(p)
    except ValueError:
        print("[ERR] Invalid format of m, k, p parameters. Required int, int and float")
        sys.exit(-1)
            
    print(f"[LOG] master: {master}")
    print(f"[LOG] input_file_path: {input_hdfs_path}")
    print(f"[LOG] output_file_path: {output_hdfs_path}")
    print(f"[LOG] m list: {m_list}")
    print(f"[LOG] k: {k}")
    print(f"[LOG] p: {p}")
    
    #Get ratings input from HDFS
    rdd_input = sc.textFile(input_hdfs_path)
    
    # Remove header from input file
    header = rdd_input.first()
    rows = rdd_input.filter(lambda line: line != header)
    
    # Parse input, group keys together
    rows_grouped = rows.map(mapRatingMovie).groupByKey()
    
    # Create and populate bloomFilters per rating
    rows_mapped = rows_grouped.map(mapBF)
    
    # Merge bloomFilters with same rating
    rows_reduced = rows_mapped.reduceByKey(reduce_bloomfilters)

    # Prepare output to be written into HDFS
    rows_reduced_sorted = rows_reduced.sortByKey()
    output_rdd = rows_reduced_sorted.map(lambda x: str(x[1])) #save BloomFilter in json format
    
    # Save output on HDFS
    output_rdd.saveAsTextFile(output_hdfs_path)