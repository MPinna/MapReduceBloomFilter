# HADOOP TEST

P = 0.01

**Run on /home/hadoop**

## ComputeParams
```
> hadoop jar it/unipi/hadoop/BloomFilter/1.0/BloomFilter-1.0.jar it.unipi.hadoop.MapRedComputeParams title.ratings.tsv esameParamsHadoop1 0.01 157000
```

## BloomFilter
```
> hadoop jar it/unipi/hadoop/BloomFilter/1.0/BloomFilter-1.0.jar it.unipi.hadoop.MapRedBloomFilter title.ratings.tsv esameBloomFilterHadoop1 157000 24404 63482 171928 420900 990060 2117062 3578304 3406703 1092505 156103 7 WithBloomFilters
```

## FPR test
```
> hadoop jar it/unipi/hadoop/BloomFilter/1.0/BloomFilter-1.0.jar it.unipi.hadoop.MapRedFalsePositiveRateTest title.ratings.tsv esameFPRHadoop1 157000 esameBloomFilterHadoop1/part-r-00000 hadoop-namenode 9820
```

# SPARK TEST

P = 0.01

**Run on /home/hadoop/bloom/spark**

## ComputeParams
```
> spark-submit spark_compute_params.py yarn hadoop-namenode 9820 title.ratings.tsv esameParamsSpark1 0.01
```

## BloomFilter
```
> spark-submit --archives pyspark_venv.tar.gz#environment spark_bloomfilter.py yarn hadoop-namenode 9820 title.ratings.tsv esameBloomFilterSpark1 1 24404 63482 171928 420900 990060 2117062 3578304 3406703 1092505 156103 7 0.01 WithBloomFilters
```

## FPR test
```
> spark-submit --archives pyspark_venv.tar.gz#environment spark_FPR_test.py yarn hadoop-namenode 9820 title.ratings.tsv esameFPRSpark1  esameBloomFilterSpark1/part-00000
```