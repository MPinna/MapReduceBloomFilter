package it.unipi.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapRedBloomFilterWithIndexes
{
                                                        
    public static class MapRedBloomFilterMapper extends Mapper<, , , > // change parameters type
    {
        //TODO add setup and clean for classes
        
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            
        }
    }

    public static class MapRedBloomFilterReducer extends Reducer<IntWritable,List<IntWritable>,NullWritable,BloomFilter> { // change parameters type

        //TODO add setup and clean for classes

        public void reduce(final IntWritable key, final Iterable<List<IntWritable>> values, final Context context)
                throws IOException, InterruptedException {
                    //Create BloomFilter for the given rating
                    int rating = key.get();
                    int m = 2000; //Hardcoded for now
                    int k = 3; //Hardcoded for now
                    float p = 0.01; //Hardcoded for now

                    BloomFilter bloomfilter = new BloomFilter(rating, m, k, p);
                    
                    //Add indexes
                    for(List<IntWritable> value: values){
                        for(IntWritable index: value){
                            bloomfilter.setAt(index.get())
                        }
                    }

                    context.write(null, bloomfilter)
        }
    }

    public static void main(final String[] args) throws Exception {


        final Configuration conf = new Configuration();
        final Job job = new Job(conf, "bloomfilter");
        job.setJarByClass(MapRedBloomFilterWithIndexes.class);

        //TODO To change
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(MapRedBloomFilterMapper.class);
        job.setReducerClass(MapRedBloomFilterReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
