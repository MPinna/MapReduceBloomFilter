import re
import sys
import decimal
import numpy as np
import util
from pyspark import SparkContext

MASTER_TYPES = ("yarn", "local", "local[*]", "local[N]")
MASTER_TYPES_REGEX = "^(yarn|local|local\[([\*]|[1-9][0-9]*)\])$"


"""
Extract rate from line whose format is:
id  rate    num_votes
"""
def getRate(line:str):
    #Split is performed by default on spaces, \t and \n (even consecutive)
    return line.split()[1]



if __name__ == "__main__":
    # Check all the arguments are present
    if len(sys.argv) != 5 and len(sys.argv) != 6:
        print("Usage: SparkComputeParams <master> <input_file> <output_file> <p> [<max_k>]", file=sys.stderr)
        sys.exit(-1)
    
    # Extract master flag and check its validity by means RegEx
    master = sys.argv[1]
    if not re.search(MASTER_TYPES_REGEX,master):
        print("Invalid master type. Select one from {0}".format(MASTER_TYPES),  file=sys.stderr)
        sys.exit(-1)

    # Initializing a SparkContext
    sc = SparkContext(master, "SparkComputeParams")

    # Get requested false positive rate
    p = float(sys.argv[4])

    # Get input file
    input = sc.textFile(sys.argv[2])

    # Remove header from input file
    header = input.first()
    rows = input.filter(lambda line: line != header)

    # From each row create key-value pairs (rounded_rate, 1)
    rawRates = rows.map(getRate)
    rates = rawRates.map(lambda x: (roundHalfUp(x),1))

    # For each key obtain the total number of occurrences
    counters = rates.reduceByKey(lambda x, y: x + y)

    # Perform action
    rateAndCountPairs = counters.collect()
    
    # For each rate compute k and m parameters
    output=[]
    for (rate, n) in rateAndCountPairs:
        if len(sys.argv) == 6:
            # k is contrained and passed as argument
            k = int(sys.argv[5])
            m = np.ceil(-(k*n)/np.log(1-np.power(p, 1/k)))
        else:
            m = np.ceil((n * np.log(p)) / np.log(1 / np.power(2, np.log(2))))
            k = roundHalfUp(np.log(2)*m/n)
        # Collect results in a list    
        output.append("{0}\t{1}\t{2}\t{3}\t{4}".format(rate,p,n,m,k))

    # Write result in output file received as argument
    sc.parallelize(output).saveAsTextFile(sys.argv[3])
