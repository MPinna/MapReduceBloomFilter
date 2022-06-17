package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapRedBloomFilter
{
    public static class MapRedBloomFilterMapper extends Mapper<Object, Text, IntWritable, BloomFilter>
    {
        private final static int N_RATINGS = 10;
        private static float P;
        private static int M;
        private static int K;
        
        private BloomFilter[] bloomFilters;
        private static IntWritable outKey = new IntWritable();
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Init bloom filters
            bloomFilters = new BloomFilter[N_RATINGS];
            for (int i = 0; i < N_RATINGS; ++i){
                bloomFilters[i] = new BloomFilter(i+1, M, K, P);
            }

            // Set parameters (from job configuration)
            K =  context.getConfiguration().getInt("k_param", 5);
            M =  context.getConfiguration().getInt("m_param", 40960);
            P =  context.getConfiguration().getFloat("p_param", 0.01f);
        }
    
        @Override
        public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException 
        {
            // Get raw input values
            String tokens[] = value.toString().split("\t+");
            
            // Check format (last value is discarded)
            if (tokens.length != 3) {
                return;
            }

            String movieId = null;
            int index = -1;
            
            // Get movie id
            movieId = tokens[0];
            if(movieId == null){
                return;
            }

            // Get rating
            try{
                float rating = Float.parseFloat(tokens[1]); 
                index = Math.round(rating);
                if (index < 1 || index > 10){
                    return;
                }
            }
            catch(NumberFormatException e){
                return;
            }
            
            // Update bloom filter
            bloomFilters[index-1].add(movieId);
            
        }

        /*
         * The 10 bloom filters are emitted just once at the end of map task
         */
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (int i = 0; i < N_RATINGS; ++i){
                outKey.set(i + 1);
                context.write(outKey,  bloomFilters[i]);
            }
        }

    }
    
    public static class MapRedBloomFilterReducer extends Reducer<IntWritable, BloomFilter, IntWritable, BloomFilter> {

        private static int k;
        private static int m;
        private static float p;

        @Override
        public void reduce(IntWritable key, Iterable<BloomFilter> values, final Context context)
                throws IOException, InterruptedException 
        {


            k = context.getConfiguration().getInt("k_param", 5);
            m = context.getConfiguration().getInt("m_param", 40960000); // 5MiB
            p = context.getConfiguration().getFloat("p_param", (float) 0.001);

            int rating = key.get();
            BloomFilter bloomFilter = new BloomFilter(rating, m, k, p);

            for (BloomFilter value : values) {
                bloomFilter.or(value);
            }

            context.write(key, bloomFilter);
        }
    }

    public static void main(final String[] args) throws Exception {

        final Configuration conf = new Configuration();
        final Job job = new Job(conf, "bloomfilter");
        
        job.setJarByClass(MapRedBloomFilter.class);

        // Read job parameters 
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
           System.err.println("Usage: BloomFilter <input> <output>");
           System.exit(1);
        }

        System.out.println("args[0]: <input>="  + otherArgs[0]);
        System.out.println("args[1]: <output>=" + otherArgs[1]);

        //TODO To change (from command line ?)
        //Set the BloomFilter parameters
        job.getConfiguration().set("k_param", "5");
        job.getConfiguration().set("m_param", "40960000"); // 5 MiB
        job.getConfiguration().set("p_param", "0.001");
        
        // Config mapper 
        job.setMapperClass(MapRedBloomFilterMapper.class);
        job.setMapOutputKeyClass(IntWritable.class); //Valid both for mapper and reducer
        job.setMapOutputValueClass(BloomFilter.class);
    
        //Config reducer
        job.setNumReduceTasks(1); //TODO to change
        job.setReducerClass(MapRedBloomFilterReducer.class);
        job.setOutputKeyClass(IntWritable.class); //TODO: depends by reducer
        job.setOutputValueClass(BloomFilter.class); //TODO: depends by reducer
       
        // Config input/output
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job, 157000); // TODO: change this value     

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
