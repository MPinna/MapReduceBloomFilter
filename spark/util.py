import decimal
import re

MASTER_TYPES = ("yarn", "local", "local[*]", "local[N]")
MASTER_TYPES_REGEX = "^(yarn|local|local\[([\*]|[1-9][0-9]*)\])$"
ROW_TYPES_REGEX = "^tt[0-9]+\t([1-9]\.[0-9]|10\.0)\t[1-9][0-9]*$"
NUM_OF_PARTITIONS = 8


"""
Compute numerical round to integer in which *.5 is rounded to upper integer 
"""
def roundHalfUp(rawRate:str):
    rate = int(decimal.Decimal(rawRate).quantize(decimal.Decimal('1'), rounding=decimal.ROUND_HALF_UP))
    return rate

"""
Check if input row is valid and well formed 
"""
def removeHeaderAndMalformedRows(row:str):
    return True if re.search(ROW_TYPES_REGEX,row) else False