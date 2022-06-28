from operator import index
import sys
import re
from util import *
from pyspark import SparkContext
from pyspark.resultiterable import ResultIterable
from BloomFilter import BloomFilter

''''''''''''''''''''''''''''''''''''''''''''''''''' REDUNDANT '''''''''''''''''''''''''''''''''''''''''''''''''''
if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) != 18:
        print("Usage: > spark-submit spark_bloomfilter_indexes.py <master> <input_file> <output_file> <m>[10 times] <k> <p> <host> <port>", file=sys.stderr)
        sys.exit(-1)
    
    #Parse cmd line input
    master = sys.argv[1]
    if not re.search(MASTER_TYPES_REGEX,master):
        print(f"Invalid master type. Select one from {MASTER_TYPES}",  file=sys.stderr)
        sys.exit(-1)
        
    sc = SparkContext(appName="BLOOM_FILTER_INDEXES", master=master)
    input_hdfs_path = sys.argv[2]
    output_hdfs_path = sys.argv[3]
    
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
    #print(f"[LOG] output_file_path: {output_hdfs_path}")
    print(f"[LOG] m list: {m_list}")
    print(f"[LOG] k: {k}")
    print(f"[LOG] p: {p}")
           
    #Get ratings input from HDFS TODO: default partion number equals to the numbers of hdfs blocks in file, it is correct ? 
    rdd_input = sc.textFile(input_hdfs_path, 20)

    # Remove header from input file TODO: maybe we have to check all lines in map function ?
    header = rdd_input.first()
    rows = rdd_input.filter(lambda line: line != header)
   
    ''''''''''''''''''''''''''''''''''''''''''''''''''' REDUNDANT '''''''''''''''''''''''''''''''''''''''''''''''''''
    def compute_indexes(line: str):
        ''''''''''''''''''''''''''' REDUNDANT '''''''''''''''''''''''''''
        # TODO: check if line is the right format
        line_ = line.split()
        rating = roundHalfUp(line_[1]) 
        movieId = line_[2]
        ''''''''''''''''''''''''''' REDUNDANT '''''''''''''''''''''''''''
        indexes = BloomFilter.computeHash(k_br.value, movieId, m_list_br.value[rating-1])
        return [(rating, indexes[i]) for i in range(0,k_br.value)] 
    
    def fill_bloom_filter(tuple: tuple):
        rating = tuple[0]
        indexes = tuple[1]
        bloom_filter = BloomFilter(rating, m_list_br.value[rating-1], k_br.value, p_br.value)
        for index in indexes:
            bloom_filter.setAt(index)
        return bloom_filter

    
    bloom_filters = rows.flatMap(compute_indexes).distinct().groupByKey().map(fill_bloom_filter)  
   
    ''''''''''''''''''''''''''''''''''''''''''''''''''' REDUNDANT '''''''''''''''''''''''''''''''''''''''''''''''''''   
    
    # Save output on HDFS
    bloom_filters.saveAsTextFile(output_hdfs_path)
    
    ''''''''''''''''''''''''''''''''''''''''''''''''''' REDUNDANT '''''''''''''''''''''''''''''''''''''''''''''''''''
    