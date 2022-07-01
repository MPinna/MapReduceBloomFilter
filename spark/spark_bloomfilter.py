import re
import sys
import util
import os
from BloomFilter import BloomFilter
from pyspark import SparkContext
from util import *


''' WithBloomFilters functions '''

def mapBF(item):    
    rating = item[0]
    movieIds = item[1]
    bloomFilter = BloomFilter(rating, m_list_br.value[rating - 1], k_br.value, p_br.value)
    
    for movieId in movieIds: 
        bloomFilter.add(movieId)

    return (rating, bloomFilter)

def reduce_bloomfilters(bloomfilter_a: BloomFilter, bloomfilter_b: BloomFilter):
    return bloomfilter_a.orBloomFilter(bloomfilter_b)


''' WithIndexes functions '''

def compute_indexes(line: str):
    line_ = line.split()
    rating = roundHalfUp(line_[1]) 
    movieId = line_[0]
    indexes = BloomFilter.computeHash(k_br.value, movieId, m_list_br.value[rating-1])
    
    return (rating, indexes)

def reduce_indexes(indexes_a, indexes_b):
    for index in indexes_b:
        indexes_a.append(index)
    return indexes_a

def fill_bloom_filter(keyValue: tuple):
    rating = keyValue[0]
    indexes = keyValue[1]
    bloom_filter = BloomFilter(rating, m_list_br.value[rating-1], k_br.value, p_br.value)
    for index in indexes:
        bloom_filter.setAt(index)
    
    return bloom_filter


''' DRIVER '''

if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) != 20:
        print("Usage: > spark-submit spark_bloomfilter.py <master> <host> <port> <input_file> <output_file> <partitions> <m>[10 times] <k> <p> <version>", file=sys.stderr)
        sys.exit(-1)
    
    # Parse cmd line input
    master = sys.argv[1]
    if not re.search(util.MASTER_TYPES_REGEX,master):
        print("Invalid master type. Select one from {0}".format(MASTER_TYPES),  file=sys.stderr)
        sys.exit(-1)
    
    # Check version
    version = sys.argv[19]
    if not re.match(f"^({WITH_BLOOM_FILTERS}|{WITH_INDEXES})$", version):
        print(f"Invalid version. Available versions: '{WITH_BLOOM_FILTERS}', '{WITH_INDEXES}'", file=sys.stderr)
        sys.exit(-1)
    
    if master == "yarn":
        #Reuse python executable obtained from virtualenv
        os.environ['PYSPARK_PYTHON'] = './environment/bin/python'
        
    # Include all python files in pyFiles argument (python packages need to be installed via virtualenv)
    sc = SparkContext(appName="BLOOM_FILTER_" + version, master=master, pyFiles=["util.py", "BloomFilter.py"])
    host = sys.argv[2]
    port = sys.argv[3]
    base_hdfs = "hdfs://" + host + ":" + port + "/user/hadoop/"
    input_hdfs_path = base_hdfs + sys.argv[4]
    output_hdfs_path = base_hdfs + sys.argv[5]
    
    try:
        partitions=int(sys.argv[6])
        if partitions < 1:
            raise ValueError
        m_list = [int(m) for m in sys.argv[7:17]]
        m_list_br = sc.broadcast(m_list)
        k = int(sys.argv[17])
        k_br = sc.broadcast(k)
        p = float(sys.argv[18])
        p_br = sc.broadcast(p)
    except ValueError:
        print("[ERR] Invalid format of partitions, m, k, p parameters. Required int, int, int and float")
        sys.exit(-1)
            
    print(f"[LOG] master: {master}")
    print(f"[LOG] input_file_path: {input_hdfs_path}")
    print(f"[LOG] output_file_path: {output_hdfs_path}")
    print(f"[LOG] partitions: {partitions}")
    print(f"[LOG] m list: {m_list}")
    print(f"[LOG] k: {k}")
    print(f"[LOG] p: {p}")
    print(f"[LOG] version: {version}")

    # Get ratings input from HDFS
    rdd_input = sc.textFile(input_hdfs_path, partitions)
   
    # Remove header and malformed rows from input file
    rows = rdd_input.filter(removeHeaderAndMalformedRows)
    
    if version == WITH_BLOOM_FILTERS:
        # Parse input, group keys together
        rows_grouped = rows.map(mapRatingMovie).groupByKey()
        
        # Create and populate bloomFilters per rating
        rows_mapped = rows_grouped.map(mapBF)
        
        # Merge bloomFilters with same rating
        rows_reduced = rows_mapped.reduceByKey(reduce_bloomfilters)

        # Save BloomFilter in json format
        output_rdd = rows_reduced.map(lambda x: str(x[1]))
    else:
        # Compute indexes, remove duplicates and group by rating
        indexes = rows.map(compute_indexes)
        
        # Reduce indexes by key
        aggregate_indexes = indexes.reduceByKey(reduce_indexes)
                
        # Create and set bloom filters bit 
        output_rdd = aggregate_indexes.map(fill_bloom_filter)
    
    # Save output on HDFS
    output_rdd.saveAsTextFile(output_hdfs_path)