import imp
import sys
import re
from util import MASTER_TYPES, MASTER_TYPES_REGEX
from pyspark import SparkContext

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
    
    #Get ratings input from HDFS TODO: default partion number equals to the numbers of hdfs blocks in file, it is correct ? 
    rdd_input = sc.textFile(input_hdfs_path)

    # Remove header from input file TODO: maybe we have to check all lines in map function ?
    header = rdd_input.first()
    rows = rdd_input.filter(lambda line: line != header)
   
    ''''''''''''''''''''''''''''''''''''''''''''''''''' REDUNDANT '''''''''''''''''''''''''''''''''''''''''''''''''''
    
    # TODO: compute indexes, (merge) and create bloom filters
   
   
    ''''''''''''''''''''''''''''''''''''''''''''''''''' REDUNDANT '''''''''''''''''''''''''''''''''''''''''''''''''''
    
    # Prepare output to be written into HDFS
    rows_reduced_sorted = rows_reduced.sortByKey()
    output_rdd = rows_reduced_sorted.map(lambda x: str(x[1])) #save BloomFilter in json format
    
    # Save output on HDFS
    output_rdd.saveAsTextFile(output_hdfs_path)
    
    ''''''''''''''''''''''''''''''''''''''''''''''''''' REDUNDANT '''''''''''''''''''''''''''''''''''''''''''''''''''
    