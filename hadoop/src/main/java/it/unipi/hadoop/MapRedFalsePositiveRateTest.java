package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.apache.log4j.Logger;

import it.unipi.hadoop.Util.UtilityConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapRedFalsePositiveRateTest
{
    public static class MapRedFalsePositiveRateTestMapper extends Mapper<Object, Text, IntWritable, ArrayPrimitiveWritable> 
    { 
        //Since we don't know if there are all 10 bloomfilters and are ordered by rating, an hashmap is used
        private HashMap<Integer, BloomFilter> bloomFiltersByRating; 
        private Logger logger;
        private static int[] false_positive_count = new int[UtilityConstants.NUM_OF_RATES];
        private static int[] true_negative_count = new int[UtilityConstants.NUM_OF_RATES];
        
        private static String pathBloomFilterFile;
        private static String defaultFS;
        private static String defaultFSPort;
        private static FileSystem fileSystem;

        private static IntWritable key = new IntWritable();
        private static ArrayPrimitiveWritable value = new ArrayPrimitiveWritable();
        private static int[] value_not_writable = new int[2];

        @Override
        public void setup(Context context) throws IOException, InterruptedException
        {
            logger = Logger.getLogger(MapRedFalsePositiveRateTestMapper.class.getName());
            bloomFiltersByRating = new HashMap<Integer, BloomFilter>();
            pathBloomFilterFile = context.getConfiguration().get("pathBloomFiltersFile");
            defaultFS = context.getConfiguration().get("defaultFS");
            defaultFSPort = context.getConfiguration().get("defaultFSPort");

            //Load bloomFilters from HDFS
            Configuration configuration = new Configuration();
            configuration.setBoolean("fs.hdfs.impl.disable.cache", true);
            configuration.set("fs.defaultFS", "hdfs://"+defaultFS+":"+defaultFSPort);   
            fileSystem = FileSystem.get(configuration);
            FSDataInputStream inputStream = fileSystem.open(new Path(pathBloomFilterFile));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

            //Initialize BloomFilter objects
            String line = null;
            BloomFilter tempBloomFilter;
            while ((line=bufferedReader.readLine())!=null){
                tempBloomFilter = new BloomFilter(line);
                bloomFiltersByRating.put(tempBloomFilter.getRating(), tempBloomFilter);
            }
            inputStream.close();
            fileSystem.close();
        }
        
        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
                    
            Object[] tokens = Util.parseInput(value.toString());  
            if (tokens == null){
                logger.info(String.format("Invalid entry: %s", value.toString()));
                return;  
            }

            //Test movie presence on all BloomFilters
            int movieRating = (int) tokens[1];
            int currBloomFilterRating;
            BloomFilter currBloomFilter;
            for(int i=0; i<UtilityConstants.NUM_OF_RATES; i++){
                currBloomFilterRating = i+1;
                currBloomFilter = bloomFiltersByRating.get(currBloomFilterRating);
                if(currBloomFilter == null)
                    continue;
                boolean testResult = currBloomFilter.test((String)tokens[0]);
                if(testResult && (int) movieRating != currBloomFilterRating)
                    false_positive_count[i]++;
                if(currBloomFilterRating!=movieRating)
                    true_negative_count[i]++;        
            }   
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{

            //Emitted value is an ArrayPrimitiveWritable 
            //  index 0: false positive count with rating i,
            //  index 1: true negative count with rating i
            for(int i=0; i<UtilityConstants.NUM_OF_RATES; i++){
                key.set(i+1); //Ratings start from 1
                value_not_writable[0] = false_positive_count[i];
                value_not_writable[1] = true_negative_count[i];
                value.set(value_not_writable);
                context.write(key, value);
            }
        }
    }
    

    public static class MapRedFalsePositiveRateTestReducer extends Reducer<IntWritable, ArrayPrimitiveWritable, IntWritable, FloatWritable> {
        private Logger logger;

        //Reducer output value containing false positive rate
        private static final FloatWritable outputValue = new FloatWritable(); 

        @Override
        public void setup(Context context) throws IOException, InterruptedException
        {
            logger = Logger.getLogger(MapRedFalsePositiveRateTestReducer.class.getName());
        }

        public void reduce(final IntWritable key, final Iterable<ArrayPrimitiveWritable> values, final Context context)
                throws IOException, InterruptedException {
                    int rate = key.get();
                    int trueNegativeCounter = 0;
                    int falsePositiveCounter = 0;

                    for (final ArrayPrimitiveWritable val : values) {
                        int[] counter = (int[]) val.get();
                        // Check values validity
                        if(counter[0]<0 || counter[1]<0){
                            logger.info(String.format("Invalid received counters: %s, %s", String.valueOf(counter[0]), String.valueOf(counter[1])));
                            //TODO: continue or return?
                            continue;
                        }
                        // Aggregate all the counters received from each mapper for a given key
                        falsePositiveCounter += counter[0];
                        trueNegativeCounter += counter[1];
                    }

                    //Check key validity
                    if(rate < UtilityConstants.MIN_RATE || rate > UtilityConstants.MAX_RATE){
                        logger.info(String.format("Invalid received rate: %s", key.toString()));
                        return;
                    }

                    // Check not to divide by zero
                    if((trueNegativeCounter + falsePositiveCounter) == 0){
                        logger.info(String.format("Invalid number of true negative for key %s: %s", key.toString(), String.valueOf(trueNegativeCounter)));
                        return;
                    }

                    // Compute false positive rate
                    //TODO check rateItemsCount?
                    float falsePositiveRate = (float)falsePositiveCounter/(float)(trueNegativeCounter + falsePositiveCounter);
                    outputValue.set(falsePositiveRate);

                    //TODO is it ok to reuse the received key?  
                    // Emit rating and values of the false positive rate for this BloomFilter
                    context.write(key, outputValue);
        }
    }

    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        final Job job = new Job(conf, "FalsePostiveRateTest");
        
        job.setJarByClass(MapRedFalsePositiveRateTest.class);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4 && otherArgs.length != 5 && otherArgs.length != 6) {
           System.err.println("Usage: BloomFilter <input> <output> <num_lines_per_split> <path_bloom_filters_file> [<defaultFS> = \"localhost\"] [<defaultFSPort> = 9000]");
           System.exit(1);
        }
        //Print input and output file path
        System.out.println("args[0]: <input>="  + otherArgs[0]);
        System.out.println("args[1]: <output>=" + otherArgs[1]);

        // Get invocation parameters
        int numLinesPerSplit = 0;

        try{
            //Take num_lines_per_split parameter
            numLinesPerSplit = Integer.parseInt(otherArgs[2]);
            System.out.println("args[2]: <lines per split>="  + otherArgs[2]);

            //Take bloomFilterFile path parameter
            job.getConfiguration().set("pathBloomFiltersFile", otherArgs[3]);
            System.out.println("args[3]: <path_bloom_filters_file>=" + otherArgs[3]);

            //Take defaultFS parameter (if missing "localhost" will be the default value)
            if(otherArgs.length == 6){
                job.getConfiguration().set("defaultFS", otherArgs[4]);
                System.out.println("args[4]: <defaultFS>=" + otherArgs[4]);
                job.getConfiguration().set("defaultFSPort", otherArgs[5]);
                System.out.println("args[5]: <defaultFSPort>=" + otherArgs[5]);
            } else {
                job.getConfiguration().set("defaultFS", "localhost");
                System.out.println("args[4]: <defaultFS>=" + "localhost");
                job.getConfiguration().set("defaultFSPort", "9000");
                System.out.println("args[5]: <defaultFSPort>=" + "9000");

            }
        }
        catch(NumberFormatException e){
            e.printStackTrace();
            System.err.println("Usage: BloomFilter <input> <output> <num_lines_per_split> <items_count_per_rate>{10 times} <path_bloom_filters_file> [<defaultFS> = \"localhost\"] [<defaultFSPort> = 9000]");
            System.exit(1);
        }
        
        //Set mapper and reducer
        job.setMapperClass(MapRedFalsePositiveRateTestMapper.class);
        job.setReducerClass(MapRedFalsePositiveRateTestReducer.class);

        //Define type of mapper's key-value output pair
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(ArrayPrimitiveWritable.class);
        
        //Define type of reducer's key-value output pair
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);

        //Define number of reduce tasks
        job.setNumReduceTasks(UtilityConstants.NUM_REDUCERS);
        
        // Config input/output
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
       
        NLineInputFormat.setNumLinesPerSplit(job, numLinesPerSplit);     
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
