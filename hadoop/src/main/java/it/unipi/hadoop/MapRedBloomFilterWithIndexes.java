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

    public static class MapRedBloomFilterReducer extends Reducer<, , , > { // change parameters type

        //TODO add setup and clean for classes

        public void reduce(final Text key, final Iterable<???> values, final Context context)
                throws IOException, InterruptedException {

                    //TODO
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
