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

    method REDUCE(rating r, bitIndexes [b1, b2, ..., bj])

        len <- getLen(r)
        bloomFilter <- new BitArray[len]
        bloomFilter.set(allZeros)

        for bitIndex b in bitIndexes:
                bloomFilter[b] = 1

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


    method REDUCE(rating r, bloomFilters [b1, b2, ..., bj])

        len <- getLen(r)
        bloomFilter <- new BitArray[len]
        bloomFilter.set(allZeros)

        for bf in bloomFilters:
            bloomFilter <- bitwiseOr(bloomFilter, bf)

        emit(r, bloomFilter)

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

    method REDUCE(rating r, counters [c1, c2, ..., cj])
        trueNegativeCounter = 0
        falsePositiveCounter = 0
        for counter in counters:
            if(counter[0] > 0 and counter[1] > 0)
                falsePositiveCounter += counter[0]
                trueNegativeCounter += counter[1]

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


    method REDUCE(rating r, rating_counts [c1, c2 ... ck])
        p = getConfigurationParam("p")
        k = getConfigurationParam("k")

        rating_count_sum = 0
        for rating_count in rating_counts:
            rating_count_sum += rating_count

        
        if K = 0:
            bestM = ceil((rating_count_sum * log(p)) / log(1 / 2**log(2)))
            bestK = round(log(2)*bestM/rating_count_sum)
        else:
            bestK = k
            bestM = ceil(-(k*rating_count_sum)/log(1-p**(1/k)))
        
        emit(r, (r, p, rating_count_sum, bestM, bestK))
```