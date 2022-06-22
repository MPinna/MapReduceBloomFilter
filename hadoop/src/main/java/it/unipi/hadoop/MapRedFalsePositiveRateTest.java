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
        private static int[] film_by_rating_count = new int[UtilityConstants.NUM_OF_RATES];
        
        private static String pathBloomFilterFile;
        private static String defaultFS;
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
            
            //Load bloomFilters from HDFS
            Configuration configuration = new Configuration();
            configuration.setBoolean("fs.hdfs.impl.disable.cache", true);
            configuration.set("fs.defaultFS", "hdfs://"+defaultFS+":9000");   
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
            for(int i=0; i<UtilityConstants.NUM_OF_RATES; i++){
                currBloomFilterRating = i+1;
                boolean testResult = bloomFiltersByRating.get(currBloomFilterRating).test((String)tokens[0]);
                if(testResult && (int) movieRating != currBloomFilterRating)
                    false_positive_count[i]++;
            }

            film_by_rating_count[(int) movieRating-1]++;
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{

            //Emitted value is an ArrayPrimitiveWritable 
            //  index 0: false positive count with rating i,
            //  index 1: total movie count with rating i
            for(int i=0; i<false_positive_count.length; i++){
                key.set(i);
                value_not_writable[0] = false_positive_count[i];
                value_not_writable[1] = film_by_rating_count[i];
                value.set(value_not_writable);
                context.write(key, value);
            }
        }
    }
    

    public static class MapRedFalsePositiveRateTestReducer extends Reducer<IntWritable, IntWritable, IntWritable, FloatWritable> {
        private Logger logger;

        private static final int[] rateItemsCount = new int[UtilityConstants.NUM_OF_RATES];

        //Reducer output value containing false positive rate
        private static final FloatWritable outputValue = new FloatWritable(); 

        @Override
        public void setup(Context context) throws IOException, InterruptedException
        {
            logger = Logger.getLogger(MapRedFalsePositiveRateTestReducer.class.getName());

            // Get number of items for each rate (from job configuration)
            for (short i = 0; i<UtilityConstants.NUM_OF_RATES; ++i)
                rateItemsCount[i] = context.getConfiguration().getInt("rateCount"+(i+1), UtilityConstants.DEFAULT_COUNT_PER_RATE);
        }

        public void reduce(final IntWritable key, final IntWritable values, final Context context)
                throws IOException, InterruptedException {
                    int rate = key.get();
                    int countOfPresentItems = values.get();

                    //Check for key and value
                    if(rate < UtilityConstants.MIN_RATE || rate > UtilityConstants.MAX_RATE){
                        logger.info(String.format("Invalid received rate: %s", key.toString()));
                        return;
                    }
                    if(countOfPresentItems<0){
                        logger.info(String.format("Invalid received count of present items: %s", values.toString()));
                        return;
                    }

                    // Compute false positive rate
                    //TODO check rateItemsCount?
                    float falsePositiveRate = (float)countOfPresentItems/(float)rateItemsCount[rate - 1];
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
        if (otherArgs.length != 14 && otherArgs.length != 15) {
           System.err.println("Usage: BloomFilter <input> <output> <num_lines_per_split> <items_count_per_rate>{10 times} <path_bloom_filters_file> [<defaultFS> = \"localhost\"]");//TODO 10 times or 10 ?
           System.exit(1);
        }
        //Print input and output file path
        System.out.println("args[0]: <input>="  + otherArgs[0]);
        System.out.println("args[1]: <output>=" + otherArgs[1]);

        // Get invocation parameters
        int numLinesPerSplit = 0;
        int[] itemsCountPerRate = new int[UtilityConstants.NUM_OF_RATES];

        try{
            //Take num_lines_per_split parameter
            numLinesPerSplit = Integer.parseInt(otherArgs[2]);
            System.out.println("args[2]: <lines per split>="  + otherArgs[2]);
            
            //Take ten values of m parameter
            for(short i = 0; i<UtilityConstants.NUM_OF_RATES; ++i){
                itemsCountPerRate[i] = Integer.parseInt(otherArgs[3+i]);
                //TODO: Here or in another for loop
                //items count value for each rating passed as configuration parameters
                job.getConfiguration().set("rateCount"+(i+1), String.valueOf(itemsCountPerRate[i]));
                
                System.out.println("args["+(3+i)+"]: <items_Count_Per_Rate_"+(i+1)+">="  + otherArgs[3+i]);
            }

            //Take bloomFilterFile path parameter
            job.getConfiguration().set("pathBloomFiltersFile", otherArgs[13]);
            System.out.println("args[13]: <path_bloom_filters_file>=" + otherArgs[13]);

            //Take defaultFS parameter (if missing "localhost" will be the default value)
            if(otherArgs.length == 15){
                job.getConfiguration().set("defaultFS", otherArgs[14]);
                System.out.println("args[14]: <defaultFS>=" + otherArgs[14]);
            } else {
                job.getConfiguration().set("defaultFS", "localhost");
            }
        }
        catch(NumberFormatException e){
            e.printStackTrace();
            System.err.println("Usage: BloomFilter <input> <output> <num_lines_per_split> <items_count_per_rate>{10 times} <path_bloom_filters_file> [<defaultFS> = \"localhost\"]");//TODO 10 times or 10 ?
            System.exit(1);
        }
        
        //Set mapper and reducer
        job.setMapperClass(MapRedFalsePositiveRateTestMapper.class);
        job.setReducerClass(MapRedFalsePositiveRateTestReducer.class);

        //Define type of mapper's key-value output pair
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        
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
