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
    public static class MapRedFalsePositiveRateTestMapper extends Mapper<Object, Text, IntWritable, IntWritable> 
    { 
        //Since we don't know if there are all 10 bloomfilters and are ordered by rating, an hashmap is used
        private HashMap<Integer, BloomFilter> bloomFiltersByRating; 
        private Logger logger;
        private int[] false_positive_count;
        private static String pathBloomFilterFile;

        @Override
        public void setup(Context context) throws IOException, InterruptedException
        {
            logger = Logger.getLogger(MapRedFalsePositiveRateTestMapper.class.getName());
            false_positive_count = new int[UtilityConstants.NUM_OF_RATES];
            bloomFiltersByRating = new HashMap<Integer, BloomFilter>();
            pathBloomFilterFile = context.getConfiguration().get("pathBloomFiltersFile");

            //Load bloomFilters from HDFS
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", "hdfs://localhost:9000");   
            FileSystem fileSystem = FileSystem.get(configuration);
            FSDataInputStream inputStream = fileSystem.open(new Path(pathBloomFilterFile));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

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
            for(int i=1; i<=UtilityConstants.NUM_OF_RATES; i++){
                boolean testResult = bloomFiltersByRating.get(i).test((String)tokens[0]);
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
        if (otherArgs.length != 16) {
           System.err.println("Usage: BloomFilter <input> <output> <num_lines_per_split> <items_count_per_rate>{10 times} <path_bloom_filters_file>");//TODO 10 times or 10 ?
           System.exit(1);
        }
        //Print input and output file path
        System.out.println("args[0]: <input>="  + otherArgs[0]);
        System.out.println("args[1]: <output>=" + otherArgs[1]);
        System.out.println("args[13]: <path_bloom_filters_file>=" + otherArgs[13]);

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
        }
        catch(NumberFormatException e){
            e.printStackTrace();
            System.err.println("Usage: BloomFilter <input> <output> <num_lines_per_split> <items_count_per_rate>{10 times}");
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
