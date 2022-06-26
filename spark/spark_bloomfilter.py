import sys
import util
from bloomfilter import BloomFilter
from pyspark import SparkContext, rdd
from util import *


def mapBF(line: str):    
    line_ = line.split()
    rating = roundHalfUp(line_[1]) 
    movieId = line_[2]
    
    #TODO very costly I guess
    bloomFilter = BloomFilter(rating, 100, 7, 0.01)
    bloomFilter.add(movieId)
    return (rating, bloomFilter)


def reduce_bloomfilters(bloomfilter_a: BloomFilter, bloomfilter_b: BloomFilter):
    return bloomfilter_a.orBloomFilter(bloomfilter_b)

if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) != 17:
        print("Usage: > spark-submit spark_bloomfilter.py <master> <input_file> <output_file> <m>[10 times] <k> <host> <port>", file=sys.stderr)
        sys.exit(-1)
    
    
    master = sys.argv[1]
    host = sys.argv[15]
    port = sys.argv[16]
    base_hdfs = "hdfs://" + host + ":" + port + "/"
    input_hdfs_path = base_hdfs + sys.argv[2]
    output_hdfs_path = base_hdfs + sys.argv[3]
    
    m_list = sys.argv[4:14]
    k = sys.argv[14]
    
    print(f"[LOG] master: {master}")
    print(f"[LOG] input_file_path: {input_hdfs_path}")
    print(f"[LOG] output_file_path: {output_hdfs_path}")
    print(f"[LOG] m list: {m_list}")
    print(f"[LOG] k: {k}")
    
    
    sc = SparkContext(appName="BLOOM_FILTER", master=master)
    
    rdd_input = sc.textFile(input_hdfs_path)
    
    # Remove header from input file
    header = rdd_input.first()
    rows = rdd_input.filter(lambda line: line != header)
    
    # Create and populate bloomFilters per rating
    rows_mapped = rows.map(mapBF)
    
    rows_reduced = rows_mapped.reduceByKey(reduce_bloomfilters)
    
    print(f"[LOG] {rows_mapped.collect()}")
    print("******************************************************************\n"*10)
    print(f"[LOG] {rows_reduced.collect()}")
     