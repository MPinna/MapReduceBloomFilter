# MapRedBloomFilter


## How to run Hadoop project


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


## Utility

To combine an hadoop output in a unique file:

```
> hadoop fs -getmerge <output_dir> <local_file>
> hadoop fs -put <local_file> <hdfs_file>
```