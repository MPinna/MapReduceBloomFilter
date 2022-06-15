package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapRedBloomFilter
{
    
    private final static int N_RATINGS = 10;
    private final static int M = 1 << 13;
    private final static int K = 3;
    private final static float P = 0.01f;

    public static class MapRedBloomFilterMapper extends Mapper<LongWritable, Text, IntWritable, BloomFilter>
    {
        private static BloomFilter[] bloomFilters;
        private static IntWritable outKey = new IntWritable();
        
        // TODO: is it correct ?
        static {
            bloomFilters = new BloomFilter[N_RATINGS];

            for (int i = 0; i < N_RATINGS; ++i){
                bloomFilters[i] = new BloomFilter(i+1, M, K, P);
            }
        }

        /*@Override
        protected void setup(Context context) throws IOException, InterruptedException {
            for (BloomFilter bloomFilter: bloomFilters){
                // Reuse the same bloom-filter
                bloomFilter.clear();
            }
        }*/
    
        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException 
        {
            // Get raw input values
            String values[] = value.toString().split("\t+");
            String movieId = values[0];
            float rating = Float.parseFloat(values[1]); 
            int index = Math.round(rating);
            
            // Set key-value
            bloomFilters[index-1].add(movieId);
            key.set(index);

            // Emit partial bloom filer 
            context.write(outKey, bloomFilters[index-1]);

        }

    }
    
    public static class MapRedBloomFilterReducer extends Reducer<IntWritable, BloomFilter, IntWritable, Text> { // change parameters type

        public void reduce(IntWritable key, Iterable<BloomFilter> values, final Context context)
                throws IOException, InterruptedException 
        {
            // TODO
        }
    }

    public static void main(final String[] args) throws Exception {

        //TODO add setup and clean for classes

        final Configuration conf = new Configuration();
        final Job job = new Job(conf, "bloomfilter");
        
        job.setJarByClass(MapRedBloomFilter.class);
        job.setOutputKeyClass(IntWritable.class); //Valid both for mapper and reducer
        job.setMapOutputValueClass(BloomFilter.class);
        //job.setOutputValueClass(Text.class); //TODO: depends by reducer
        job.setMapperClass(MapRedBloomFilterMapper.class);
        //job.setCombinerClass(MapRedBloomFilterReducer.class);
        job.setReducerClass(MapRedBloomFilterReducer.class);
        job.setInputFormatClass(NLineInputFormat.class);
        
        NLineInputFormat.setNumLinesPerSplit(job, 500); // TODO: change this value
        NLineInputFormat.addInputPath(job, new Path(args[0]));
        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
