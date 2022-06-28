# MapRedBloomFilter

## How to run **Hadoop** project
---

> **_NOTE:_** $HADOOP_HOME/sbin must be added in the path to run the following commands


**1. Build project:**


Locally, on **hadoop/** path run:

```
> mvn clean package
```

In addition to this, you can also deploy the artifact directly on the hadoop namenode (remember to activate the VPN and perform the login when asked)

```
> mvn -s .\settings.xml clean package deploy
```

**2. Start HDFS and YARN:**

On the **namenode** run:
```
 > start-dfs.sh
 > start-yarn.sh
```



**4. Run hadoop executable:**

On the **namenode** run:

```
> hadoop jar <path_to_jar> <class> <input_file> <output_directory> [<options>]
```

## How to run **Spark** project
---

### **Locally**

Command format
```
> spark-submit <py_file> [<options>]
```

### **On Yarn Cluster**

The archive **pyspark_venv.tar.gz** contains a virtual environment with dependencies (mmh3 and bitarray) and python executable needed to run a python application in Spark.

If your driver application uses other local python files, add them via *pyFiles* parameter of SparkContext (see spark_bloomfilter.py as an example)

To run the driver program on YARN (note master should be set up as **yarn**)
```
> spark-submit --archives pyspark_venv.tar.gz#environment <py_file> [<options>]
```

If you need to add another *dependency* from the one listed in the file *requirements.txt*, follow these steps:

** 1. Create another virtualenv and activate it **

```
> python -m venv pyspark_venv
> source pyspark_venv/bin/activate
```
** 2. Install all needed dependencies **
```
> pip install <name>
```
** 3. Create a tar.gz archive (need venv-pack to be installed with pip) **
```
> venv-pack -o pyspark_venv.tar.gz
```

## Utility

To combine an hadoop output in a unique file:

```
> hadoop fs -getmerge <output_dir> <local_file>
> hadoop fs -put <local_file> <hdfs_file>
```
---
---
---
# Configurations

## KPIs:
    * Execution time
    * Amount of traffic on the network from mappers to reducers
    * Amount of data written on HDFS from reducers

## Constants:
    * N

## Parameters:
    * p {0.001%, 0.01%, 0.1%, 1%, 10%}
    * Number of lines of input as one split (and consequently number of mappers {2, 4, 6, 8, 10, 12})
    
    And eventually, keeping all the rest constants:
    * Number of reducers {1, 2, 5, 10}

## Other stuff
    * M as function of computeParams(p) (with and without constraint on maxK {2,10,2})
    * k 



```python
mapper:

    bloomfilters[10]
    test(movie)
        for i in range(10):
            res = bloomfilters[i].test(movie)
            emit(i, res)
```