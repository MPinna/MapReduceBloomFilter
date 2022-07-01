from BloomFilter import BloomFilter
import mmh3
from bitarray import bitarray
import util

def printBloomFilter(bloom: BloomFilter):
    print(f"Rating: {bloom.rating}")
    print(f"m: {bloom.m}")
    print(f"K: {bloom.K}")
    print(f"P: {bloom.P}")
    print(f"bitarray: {bloom.bitArray}")

bloom = BloomFilter(1, 10, 1, 1)
bloom.add("tt000001")
assert bloom.test("tt000001") == True

bloom2 = BloomFilter(1, 10, 1, 1)
bloom2.add("tt000005")
bloom2.add("tt000006")
bloom2.add("tt000007")
bloom2.add("tt000008")
bloom2.add("tt000009")
bloom2.add("tt000010")
assert bloom2.test("tt000005") == True

bloom.orBloomFilter(bloom2)
assert bloom.test("tt000001") == True
assert bloom.test("tt000005") == True

str_bloom = str(bloom)
printBloomFilter(bloom)
bloom = BloomFilter(json_string=str_bloom)
assert bloom.test("tt000001") == True
assert bloom.test("tt000005") == True
assert bloom.test("tt000006") == True
assert bloom.test("tt000007") == True
assert bloom.test("tt000008") == True
assert bloom.test("tt000009") == True
assert bloom.test("tt000010") == True
print("Test Finished Correctly.")
 
# n = 4000
# m = 19000
# k = 3
# => p = 0.1

bloom_fpr_test = BloomFilter(1, 19000, 3, 0.1)
bloom_fpr_test2 = BloomFilter(1, 19000, 3, 0.1)

FP = 0
TN = 0

for i in range(2000):
    bloom_fpr_test.add("test" + str(i))
for i in range(2000, 4000):
    bloom_fpr_test2.add("test" + str(i))

bloom_fpr_test = bloom_fpr_test.orBloomFilter(bloom_fpr_test2)

for i in range(4000, 24000):
    res = bloom_fpr_test.test("test" + str(i))
    if res:
        FP += 1
    else:
        TN += 1
        
FPR = FP/(FP+TN)
print("FPR (~0.10 expected): " + str(FPR))

bloom_fpr_test3 = BloomFilter(json_string=str(bloom_fpr_test))

FP = 0
for i in range(4000, 24000):
    res = bloom_fpr_test.test("test" + str(i))
    if res:
        FP += 1
FPR = FP/(FP+TN)
print("FPR (~0.10 expected) " + str(FPR))
