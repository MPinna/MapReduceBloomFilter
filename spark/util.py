import decimal

MASTER_TYPES = ("yarn", "local", "local[*]", "local[N]")
MASTER_TYPES_REGEX = "^(yarn|local|local\[([\*]|[1-9][0-9]*)\])$"


"""
Compute numerical round to integer in which *.5 is rounded to upper integer 
"""
def roundHalfUp(rawRate:str):
    rate = int(decimal.Decimal(rawRate).quantize(decimal.Decimal('1'), rounding=decimal.ROUND_HALF_UP))
    return rate
