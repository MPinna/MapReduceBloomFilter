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

# def FPR_map(item: tuple):
#     bloom_filters_by_rating = {}
#     false_positive_count = [0]*NUM_OF_RATINGS
#     true_negative_count = [0]*NUM_OF_RATINGS

#     # initialize collection of bloom filters
#     for line in bloom_filters_list:
#         temp_bloom_filter: BloomFilter = BloomFilter(json_string=line.strip())
#         printBloomFilter(temp_bloom_filter)
#         # print(f"[LOG] Rating: {temp_bloom_filter.rating}")
#         bloom_filters_by_rating[temp_bloom_filter.rating] = temp_bloom_filter

#     print(f"[LOG] bloom_filters_by_rating: {bloom_filters_by_rating}")
#     movie_rating = item[0]
#     movie_ids = item[1]

#     for i in range(NUM_OF_RATINGS):
#         curr_bloom_filter_rating = i+1
#         curr_bloom_filter: BloomFilter = bloom_filters_by_rating.get(curr_bloom_filter_rating)
#         if(curr_bloom_filter == None):
#             print(f"[LOG] ##########################\n")    
#             continue
#         for movie_id in movie_ids:
#             movie_in_filter = curr_bloom_filter.test(movie_id)
#             if(movie_in_filter and movie_rating != curr_bloom_filter_rating):
#                 false_positive_count[i] += 1
#             if(curr_bloom_filter_rating != movie_rating):
#                 true_negative_count[i] += 1


#     return [(i + 1, (false_positive_count[i], true_negative_count[i])) for i in range(NUM_OF_RATINGS)]

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
TN = 20000

for i in range(2000):
    bloom_fpr_test.add("test" + str(i))
for i in range(2000, 4000):
    bloom_fpr_test2.add("test" + str(i))

bloom_fpr_test = bloom_fpr_test.orBloomFilter(bloom_fpr_test2)

for i in range(4000, 24000):
    res = bloom_fpr_test.test("test" + str(i))
    if res:
        FP += 1
        
FPR = FP/(FP+TN)
print("FPR (~0.10 expected): " + str(FPR))
print("FP: " + str(FP))
print("TN: " + str(TN))


bloom_fpr_test3 = BloomFilter(json_string=str(bloom_fpr_test))

FP = 0
for i in range(4000, 24000):
    res = bloom_fpr_test.test("test" + str(i))
    if res:
        FP += 1
FPR = FP/(FP+TN)
print("FPR (~0.10 expected) " + str(FPR))
print("FP: " + str(FP))
print("TN: " + str(TN))


#Another test

# bloomFilters = []
# for i in range(10):
#     bloomFilters.append(BloomFilter(i+1, 1000, 3, 0.01))

# lines = []
# lines_formatted = []
# with open("../dataset/data10000.tsv") as fp:
#     lines = fp.readlines()
    
# # print(lines[1:])
# for line in lines[1:]:
#     line_ = line.split()
#     rating = util.roundHalfUp(line_[1]) 
#     movieId = line_[0]
#     lines_formatted.append((rating, movieId))
    
# print(lines_formatted[:])