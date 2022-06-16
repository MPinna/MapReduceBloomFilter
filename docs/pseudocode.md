# MapReduce implementation of a BloomFilter in two different versions
## MapReduceBloomFilter V.1
### Mapper: Mapper with indexes
```python
class BLOOMFILTERMAPPER

    method MAP(splitid a, split s)

        for all movie m in split s do
            rating <- round(m.rating)
            id <- m.id
            len <- getLen(rating)

            for i in range(k):
                bitIndex[i] <- (hash_i(m.id) % len)

            emit(rating, bitIndex)

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

        emit(rating r, bloomFilter bloomFilter)

```
___
## MapReduceBloomFilter V.2
### Mapper: Mapper with BloomFilter
```python
class BLOOMFILTERMAPPER

    method MAP(splitid a, split s)

        for i in range(1, 11)
            len <- getLen(i)
            bloomFilter_i <- new BitArray[len]
            bloomFilter_i.set(allZeros)


        for all movie m in split s do
            rating <- round(m.rating)
            id <- m.id
            len <- getLen(rating)

            for i in range(k):
                bitIndex <- (hash_i(m.id) % len)
                bloomFilter_rating[bitIndex] = 1

        for rating in range(1, 11)
            emit(rating, bloomFilter_i)

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

        emit(rating r, bloomFilter bloomFilter)

```
___

## Tester
