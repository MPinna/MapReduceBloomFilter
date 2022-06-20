package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


final class UtilityConstants{
    //Constant value for number of possible different rounded rates
    public static final int NUM_OF_RATES = 10;

    //Constant value for BloomFilter required false positive rate
    public static final float FALSE_POSITIVE_RATE = (float) 0.01; //1%
    
    //Constant values for implementation versions name
    public static final String[] NAME_OF_VERSIONS = {"WithIndexes","WithBloomFilters"};    
    
    //Constant defualt values for K and M BloomFilter parameters
    public static final int DEFAULT_K= 5;
    public static final int DEFAULT_M = 40960000; // 5 MiB
    
    //Constant value for number of reduce tasks
    public static final int NUM_REDUCERS = 3;
}


public class MapRedBloomFilterWithIndexes
{
                                                        
    public static class MapRedBloomFilterMapper extends Mapper<Object, Text, IntWritable, ArrayPrimitiveWritable> 
    {
        // Number of hash functions to be computed
        private static int k;
        // Vector containg the dimensions in bit of each BloomFilter
        private static final int[] m = new int[UtilityConstants.NUM_OF_RATES];

        // Implementation version to be executed
        private static String version;

        //Reuse writable key and value obj
        private static final IntWritable rate = new IntWritable();
        private static final ArrayPrimitiveWritable hashesValue = new ArrayPrimitiveWritable();

        public void setup(Context context) throws IOException, InterruptedException
        {
            // Set k and m BloomFilter parameters (from job configuration)
            k = context.getConfiguration().getInt("k_param", UtilityConstants.DEFAULT_K);
            for (int i = 0; i< UtilityConstants.NUM_OF_RATES; ++i)
                m[i] = context.getConfiguration().getInt("m_param_rate_"+(i+1), UtilityConstants.DEFAULT_M);
            
            // Set version parameter (from job configuration)
            version = context.getConfiguration().get("version", UtilityConstants.NAME_OF_VERSIONS[0]);
        }

        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            
                    // Get input values and check correctness in length and type
                    String record = value.toString();
                    if (record == null || record.length() == 0)
                        return;

                    String[] tokens = record.split("\t+");

                    if (tokens.length == 3) {
                        String movieId = null;
                        float rawRate;

                        movieId= tokens[0];
                        if(movieId == null){
                            return;
                        }

                        try{
                            rawRate = Float.parseFloat(tokens[1]);
                        }
                        catch(NumberFormatException e){
                            return;
                        }
                        if (rawRate < 1.0 || rawRate > 10.0){
                            return;
                        }
                        
                        // Compute rounded rate
                        int roundedRate = Math.round(rawRate);

                        // Compute k hash functions
                        int[] hashValue = BloomFilter.computeHash(k, movieId, m[roundedRate - 1]);

                        // Set Map key
                        rate.set(roundedRate);

                        // Set Map value
                        hashesValue.set(hashValue);

                        // Emit rating e values of the k hash functions
                        context.write(rate, hashesValue);
                    }
        }
    }


    public static class MapRedBloomFilterReducer extends Reducer<IntWritable,ArrayPrimitiveWritable,NullWritable,BloomFilter> {
        // Number of hash functions to be computed
        private static int k;
        // Vector containg the dimensions in bit of each BloomFilter
        private static final int[] m = new int[UtilityConstants.NUM_OF_RATES];
        
        // Utility attribute for code readability
        private static final float p = UtilityConstants.FALSE_POSITIVE_RATE;

        // Implementation version to be executed
        private static String version;

        private BloomFilter bloomFilter;

        public void setup(Context context) throws IOException, InterruptedException
        {
            // Set k and m BloomFilter parameters (from job configuration)
            k = context.getConfiguration().getInt("k_param", UtilityConstants.DEFAULT_K);
            for (int i = 0; i< UtilityConstants.NUM_OF_RATES; ++i)
                m[i] = context.getConfiguration().getInt("m_param_rate_"+(i+1), UtilityConstants.DEFAULT_M);
            
            // Set version parameter (from job configuration)
            version = context.getConfiguration().get("version", UtilityConstants.NAME_OF_VERSIONS[0]);
        }

        public void reduce(final IntWritable key, final Iterable<ArrayPrimitiveWritable> values, final Context context)
                throws IOException, InterruptedException {
                    //Create BloomFilter for the given rating
                    int rating = key.get();
                    bloomFilter = new BloomFilter(rating, m[rating - 1], k, p);
                   
                    //Add indexes
                    for(ArrayPrimitiveWritable value: values){
                        int[] indexes = (int[]) value.get();

                        for(int i=0; i<indexes.length; i++){
                            bloomFilter.setAt(indexes[i]);
                        }
                    }

                    context.write(null, bloomFilter);
        }
    }

    public static void main(final String[] args) throws Exception {


        final Configuration conf = new Configuration();
        final Job job = new Job(conf, "BloomFilter");
        job.setJarByClass(MapRedBloomFilterWithIndexes.class);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 15) {
           System.err.println("Usage: BloomFilter <input> <output> <num_lines_per_split> <m_value>{10 times} <k_value> <version>");
           System.exit(1);
        }
        //Print input and output file path
        System.out.println("args[0]: <input>="  + otherArgs[0]);
        System.out.println("args[1]: <output>=" + otherArgs[1]);

        // Get invocation parameters
        int numLinesPerSplit = 0;
        int[] m_value = new int[UtilityConstants.NUM_OF_RATES];
        int k_value = 0;

        try{
            //Take num_lines_per_split parameter
            numLinesPerSplit = Integer.parseInt(otherArgs[2]);
            
            //Take ten values of m parameter
            for(short i = 0; i<UtilityConstants.NUM_OF_RATES; ++i)
                m_value[i] = Integer.parseInt(otherArgs[3+i]);
            
            //Take k value
            k_value = Integer.parseInt(otherArgs[13]);
        }
        catch(NumberFormatException e){
            e.printStackTrace();
            System.err.println("Usage: BloomFilter <input> <output> <num_lines_per_split> <m_value>{10} <k_value> <version>");
            System.exit(1);
        }
        //Take implementation version to be executed
        String version = otherArgs[14];
        if(!version.equalsIgnoreCase(UtilityConstants.NAME_OF_VERSIONS[0]) &&  
            !version.equalsIgnoreCase(UtilityConstants.NAME_OF_VERSIONS[1])){
                System.err.println("Invalide <version> parameter. Options:"+
                    "\n1)"+UtilityConstants.NAME_OF_VERSIONS[0]+
                    "\n2)"+UtilityConstants.NAME_OF_VERSIONS[1]);
                System.exit(1);
            }

        //TODO To change
        //Set the BloomFilter parameters
        //K value
        job.getConfiguration().set("k_param", String.valueOf(k_value));
        //M value for each rating
        for(short i = 0; i<UtilityConstants.NUM_OF_RATES; ++i)
            job.getConfiguration().set("m_param_rate_"+(i+1), String.valueOf(m_value[i]));
        //Set the version of MapReduce implementation to be executed
        job.getConfiguration().set("version", version);

        //Set mapper and reducer
        job.setMapperClass(MapRedBloomFilterMapper.class);
        job.setReducerClass(MapRedBloomFilterReducer.class);

        //TODO To check
        //Define type of mapper's key-value output pair
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(ArrayPrimitiveWritable.class);
        
        //Define type of reducer's key-value output pair
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(BloomFilter.class);

        //TODO to change
        //Define number of reduce tasks
        job.setNumReduceTasks(UtilityConstants.NUM_REDUCERS);
        
        //Define I/O
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        //TODO to check
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        NLineInputFormat.setNumLinesPerSplit(job, numLinesPerSplit);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
