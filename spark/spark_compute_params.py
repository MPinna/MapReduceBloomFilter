import math
import re
import sys
import util
from pyspark import SparkContext


"""
Extract rate from line whose format is:
id  rate    num_votes
"""
def getRate(line:str):
    #Split is performed by default on spaces, \t and \n (even consecutive)
    return line.split()[1]

"""
Taken a tuple containing rate and number of relative items, compute m and k parameters and
return string to be saved as output of the computation
"""
def computeParams(rate_counter_tuple):
    #Extract rate and n from tuple received as argument
    rate = rate_counter_tuple[0]
    n = rate_counter_tuple[1]
    # Compute m and k parameters
    if len(sys.argv) == 8:
        # k is contrained and passed as argument
        k = k_param_br.value
        m = int(math.ceil(-(k*n)/math.log(1-math.pow(p_br.value, 1/k))))
    else:
        m = int(math.ceil((n * math.log(p_br.value)) / math.log(1 / math.pow(2, math.log(2)))))
        k = util.roundHalfUp(math.log(2)*m/n)
    # Collect results in a string   
    output = f"{rate}\t{p_br.value}\t{n}\t{m}\t{k}"
    return output


if __name__ == "__main__":
    # Check all the arguments are present
    if len(sys.argv) != 7 and len(sys.argv) != 8:
        print("Usage: > spark-submit compute_params.py <master> <host> <port> <input_file> <output_file> <p> [<max_k>]", file=sys.stderr)
        sys.exit(-1)
    
    # Extract master flag and check its validity by means RegEx
    master = sys.argv[1]
    if not re.search(util.MASTER_TYPES_REGEX,master):
        print(f"Invalid master type. Select one from {util.MASTER_TYPES}",  file=sys.stderr)
        sys.exit(-1)

    # Initializing a SparkContext
    sc = SparkContext(master, "SparkComputeParams")

    # Get values of p and possibly k and broadcast them
    try:
        # Get requested false positive rate
        p_param = float(sys.argv[6])
        p_br = sc.broadcast(p_param)
        # Get maximum value for k if passed as argument
        if len(sys.argv) == 8:
            k_param = int(sys.argv[7])
            k_param_br = sc.broadcast(k_param)
    except ValueError:
        print("[ERR] Invalid format of p and/or k parameters. Required float and int values")
        sys.exit(-1)

    # Input file and output file path
    host = sys.argv[2]
    port = sys.argv[3]
    base_hdfs = "hdfs://" + host + ":" + port + "/"
    input_hdfs_path = base_hdfs + sys.argv[4]
    output_hdfs_path = base_hdfs + sys.argv[5]

    # Get input file
    input = sc.textFile(input_hdfs_path)

    # Remove header from input file
    header = input.first()
    rows = input.filter(lambda line: line != header)

    # From each row create key-value pairs (rounded_rate, 1)
    rawRates = rows.map(getRate)
    rates = rawRates.map(lambda x: (util.roundHalfUp(x),1))

    # For each key obtain the total number of occurrences
    counters = rates.reduceByKey(lambda x, y: x + y)

    # For each rate compute k and m parameters
    bloomFilterParams = counters.map(computeParams)

    # Write result in output file received as argument
    bloomFilterParams.saveAsTextFile(output_hdfs_path)
