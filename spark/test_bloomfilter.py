from BloomFilter import BloomFilter
import mmh3
from bitarray import bitarray

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
assert bloom2.test("tt000005") == True

bloom.orBloomFilter(bloom2)
assert bloom.test("tt000001") == True
assert bloom.test("tt000005") == True

print("Test Finished Correctly.")

 

