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
Taken rate and number of relative items, compute m and k parameters and
return string to be saved as output of the computation
"""
def computeParams(rate, n):
    if len(sys.argv) == 6:
        # k is contrained and passed as argument
        k = int(sys.argv[5])
        m = int(math.ceil(-(k*n)/math.log(1-math.pow(p, 1/k))))
    else:
        m = int(math.ceil((n * math.log(p)) / math.log(1 / math.pow(2, math.log(2)))))
        k = util.roundHalfUp(math.log(2)*m/n)
    # Collect results in a string   
    output = f"{rate}\t{p}\t{n}\t{m}\t{k}"
    return output


if __name__ == "__main__":
    # Check all the arguments are present
    if len(sys.argv) != 5 and len(sys.argv) != 6:
        print("Usage: SparkComputeParams <master> <input_file> <output_file> <p> [<max_k>]", file=sys.stderr)
        sys.exit(-1)
    
    # Extract master flag and check its validity by means RegEx
    master = sys.argv[1]
    if not re.search(util.MASTER_TYPES_REGEX,master):
        print(f"Invalid master type. Select one from {util.MASTER_TYPES}",  file=sys.stderr)
        sys.exit(-1)

    # Initializing a SparkContext
    sc = SparkContext(master, "SparkComputeParams")

    # Get requested false positive rate
    p = float(sys.argv[4])

    # Get input file
    #TODO in this way or by hdfs?
    input = sc.textFile(sys.argv[2])

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
    bloomFilterParams.saveAsTextFile(sys.argv[3])
