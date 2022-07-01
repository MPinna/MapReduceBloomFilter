# MapReduce implementation of a BloomFilter in two different versions
## MapReduceBloomFilter V.1
### Mapper: Mapper with indexes
```python
class BLOOMFILTERMAPPER

    method MAP(splitid a, split s)

        for all movie m in split s:
            rating <- round(m.rating)
            id <- m.id
            len <- getBitArrayLen(rating)
            bitArrayIndexes <- new Array[k]
             
            for i in range(k):
                bitArrayIndexes[i] <- (hash_i(m.id) % len)

            emit(rating, bitArrayIndexes)

```
### Reducer
```python
class BLOOMFILTERREDUCER

    method REDUCE(rating r, bitIndexes [b1[], b2[], ..., bj[]] as bs)

        len <- getBloomFilterLen(r)
        bloomFilter <- new BitArray[len]
        bloomFilter.set(allZeros)

        for bitIndex b in bs:
            for index i in b:
                bloomFilter[i] = 1

        emit(r, bloomFilter)

```
___
## MapReduceBloomFilter V.2
### Mapper: Mapper with BloomFilter
```python
class BLOOMFILTERMAPPER

    method MAP(splitid a, split s)

        for i in range(1, 11)
            len <- getBitArrayLen(i)
            bloomFilter_i <- new BitArray[len]
            bloomFilter_i.set(allZeros)


        for all movie m in split s do
            rating <- round(m.rating)
            len <- getBitArrayLen(rating)
            bloomFilter_i.add(m.id)

        for i in range(1, max_rating)
            emit(i, bloomFilter_i)

```
### Reducer
```python
class BLOOMFILTERREDUCER


    method REDUCE(rating r, bloomFilters [b1, b2, ..., bj] as bfs)

        len <- getBloomFilterLen(r)
        bloomFilter_result <- new BitArray[len]
        bloomFilter_result.set(allZeros)

        for bloomFilter bf in bfs :
            bloomFilter_result <- bitwiseOr(bloomFilter_result, bf)

        emit(r, bloomFilter_result)

```
___

## Test
### Mapper
```python
class TESTMAPPER

    method MAP(splitid a, split s)
        savedBloomFilters <- loadBloomFilterFromHDFS()

        true_negative_count <- new int[NUM_OF_RATE]
        false_positive_count <- new int[NUM_OF_RATE]

        for all movie m in split s:
            movieRating <- round(m.rating)
            for currRating in range(1, max_rating)
                bloomFilter <- savedBloomFilters[currRating]
                test_result <- bloomFilter.test(m.id)
                if (test_result and rating != bloomFilterRating):
                    false_positive_count[bloomFilterRating -1] += 1
                if (not test_result and rating != bloomFilterRating):
                    true_negative_count[bloomFilterRating -1] += 1

        counter = new int[2]
        for bloomFilter in savedBloomFilters:
            bloomFilterRating <- bloomFilter.getRating()
            counter[0] <- false_positive_count[bloomFilterRating -1]
            counter[1] <- true_negative_count[bloomFilterRating -1]
            emit(bloomFilterRating, counter)

```
### Reducer
```python
class TESTREDUCER

    method REDUCE(rating r, counters [c1[], c2[], ..., cj[]] as cs)
        falsePositiveCounter = 0
        trueNegativeCounter = 0
        for counter c in cs:
            if(c[0] >= 0 and c[1] >= 0)
                falsePositiveCounter += c[0]
                trueNegativeCounter += c[1]

        if((trueNegativeCounter + falsePositiveCounter) != 0):
            falsePositiveRate = falsePositiveCounter/(trueNegativeCounter + falsePositiveCounter)
            emit(r, falsePositiveRate)

```


___
## ComputeParams
### Mapper:
```python
class COMPUTEPARAMSMAPPER

    method MAP(splitid a, split s)
        rating_count <- new int[NUM_RATING]
        
        for movie m in split s:
            rating_count[m.rating -1] += 1
        
        for i in range(1, max_rating):
            emit(i, rating_count[i-1]) 
```
### Reducer
```python
class COMPUTEPARAMSREDUCER

    method REDUCE(rating r, rating_counts [c1, c2 ... ck] as rc)

        rating_count_sum = 0
        for rating_count c in rc:
            rating_count_sum += c

        
        if no constraints on K :
            m = computeBestM(rating_count_sum, p)
            k = computeBestK(rating_count_sum, m)
        else:
            k = constrained_k
            m = computeBestM(rating_count_sum, p, k)
        
        emit(r, (p, rating_count_sum, m, k))
```