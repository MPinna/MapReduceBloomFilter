# MapReduceBloomFilter
# Mapper
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
# Reducer
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

# Tester
