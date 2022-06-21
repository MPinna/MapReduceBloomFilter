package it.unipi.hadoop;

import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MapRedFalsePositiveRateTest
{
    public static class MapRedFalsePositiveRateTestMapper extends Mapper<Object, Text, IntWritable, IntWritable> 
    { 
        private BloomFilter[] bloomFilters;
        private Logger logger;
        private int[] false_positive_count;

        @Override
        public void setup(Context context) throws IOException, InterruptedException
        {
            logger = Logger.getLogger(MapRedFalsePositiveRateTestMapper.class.getName());
            false_positive_count = new int[bloomFilters.length];

            //Load bloomFilters
            
        }
        
        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
                    
            Object[] tokens = Util.parseInput(value.toString());  

            if (tokens == null){
                logger.info(String.format("Invalid entry: %s", value.toString()));
                return;  
            }
    
            for(int i=0; i<bloomFilters.length; i++){
                boolean testResult = bloomFilters[i].test((String)tokens[0]);
                if(testResult)
                    false_positive_count[i]++;
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
            for(int i=0; i<false_positive_count.length; i++){
                context.write(new IntWritable(i), new IntWritable(false_positive_count[i]));
            }
        }
    }
    

    public static class MapRedFalsePositiveRateTestReducer extends Reducer<IntWritable, IntWritable, NullWritable,BloomFilter> {
        public void reduce(final IntWritable key, final Iterable<IntWritable> values, final Context context)
                throws IOException, InterruptedException {
                  
        }
    }

    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        final Job job = new Job(conf, "BloomFilter");
        
        job.setJarByClass(MapRedFalsePositiveRateTest.class);

        //Set mapper and reducer
        job.setMapperClass(MapRedFalsePositiveRateTestMapper.class);
        job.setReducerClass(MapRedFalsePositiveRateTestReducer.class);

        //Define type of mapper's key-value output pair
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        //Define type of reducer's key-value output pair
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(BloomFilter.class);

    }
}
