# MapRedBloomFilter


## How to run Hadoop project


> **_NOTE:_** $HADOOP_HOME/sbin must be added in the path to run the following commands

**1. Start HDFS:**

```
 > start-dfs.sh
```

**2. Start Yarn:**

```
 > start-yarn.sh
```

**3. Build project:**

On **/hadoop** path run:

```
> mvn clean package
```

**4. Run hadoop executable:**

```
> hadoop jar BloomFilter-1.0.jar <class> <input_file> <output_directory> [<options>]
```
