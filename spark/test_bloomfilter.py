from BloomFilter import BloomFilter
import mmh3
from bitarray import bitarray

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
# assert bloom.test("asdf") == False

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
# assert bloom.test("asdf") == False
printBloomFilter(bloom)
print("Test Finished Correctly.")
 
 
# bloom_filters_list = [str(bloom), str(bloom2)]
# # printBloomFilter(bloom)
# print(bloom_filters_list)
# NUM_OF_RATINGS = 10
# FPR_map((5, ["f1", "f2", "f3", "f4"]))
