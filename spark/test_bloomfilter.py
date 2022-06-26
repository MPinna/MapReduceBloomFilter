from bloomfilter import BloomFilter

def printBloomFilter(bloom: BloomFilter):
    print(f"Rating: {bloom.rating}")
    print(f"m: {bloom.m}")
    print(f"K: {bloom.K}")
    print(f"P: {bloom.P}")
    print(f"bitarray: {bloom.bitArray}")


bloom = BloomFilter(1, 10, 1, 1)
printBloomFilter(bloom)
bloom.add("tt000001")
printBloomFilter(bloom)
bloom.add("tt000002")
printBloomFilter(bloom)
bloom.add("tt000333")
printBloomFilter(bloom)

print("======================================\n"*3)

bloom2 = BloomFilter(1, 10, 2, 1)
printBloomFilter(bloom2)
bloom2.add("tt000001")
printBloomFilter(bloom2)
bloom2.add("tt000002")
printBloomFilter(bloom2)
bloom2.add("tt000333")
printBloomFilter(bloom2)

print("======================================\n"*3)


print(f"bitarray 1: {bloom.bitArray}")
print(f"bitarray 2: {bloom2.bitArray}")
bloom.orBloomFilter(bloom2)
print(f"bitarray 3: {bloom.bitArray}")


print("======================================\n"*3)

bloom3 = BloomFilter(1, 10, 2, 1)
bloom3.setAt(1)
printBloomFilter(bloom3)
print(bloom3.test("00000"))
print(bloom2.test("tt000001"))
bloom3.clear()
printBloomFilter(bloom3)

print("======================================\n"*3)

json_str = bloom2.__str__()
print(json_str)
print(type(json_str))

bloom4 = BloomFilter(json_string=json_str)
printBloomFilter(bloom4)
