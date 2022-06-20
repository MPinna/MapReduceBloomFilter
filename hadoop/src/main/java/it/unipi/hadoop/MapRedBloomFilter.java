package it.unipi.hadoop;
import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
    public static class MapRedBloomFilterMapperWithBloomFilters extends Mapper<Object, Text, IntWritable, BloomFilter>
    {
        private static int[] m;
        private static int k;
        
        private static BloomFilter[] bloomFilters;
        private static IntWritable rateKey = new IntWritable();
        private static Logger logger;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logger = Logger.getLogger(MapRedBloomFilterMapperWithBloomFilters.class.getName());
            m = new int[UtilityConstants.NUM_OF_RATES];
            
            // Set parameters (from job configuration)
            k = context.getConfiguration().getInt("k_param", UtilityConstants.DEFAULT_K);
            for (int i = 0; i < UtilityConstants.NUM_OF_RATES; ++i)
                m[i] = context.getConfiguration().getInt("m_param_rate_"+(i+1), UtilityConstants.DEFAULT_M);
                        
            // Init bloom filters
            bloomFilters = new BloomFilter[UtilityConstants.NUM_OF_RATES];
            for (int i = 0; i < UtilityConstants.NUM_OF_RATES; ++i){
                bloomFilters[i] = new BloomFilter(i+1, m[i], k, -1);
                logger.info(String.format("BloomFilter[%d]: <M = %d K = %d>", i+1, m[i], k));
            }
        }
    
        @Override
        public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException 
        {
            /*// Get raw input values
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
            }*/

            Object[] tokens = Util.parseInput(value.toString());  
            if (tokens == null){
                logger.info(String.format("Invalid entry: %s", value.toString()));
                return;  
            }
 
            // Update bloom filter
            bloomFilters[(int)tokens[1]-1].add((String)tokens[0]);            
        }

        /*
         * The 10 bloom filters are emitted just once at the end of map task
         */
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (int i = 0; i < UtilityConstants.NUM_OF_RATES; ++i){
                rateKey.set(i + 1);
                context.write(rateKey,  bloomFilters[i]);
            }
        }

    }
    
    public static class MapRedBloomFilterReducerWithBloomFilters extends Reducer<IntWritable, BloomFilter, IntWritable, BloomFilter> {

        private static int k;
        private static int[] m;
        private static Logger logger;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logger = Logger.getLogger(MapRedBloomFilterReducerWithBloomFilters.class.getName());
            m = new int[UtilityConstants.NUM_OF_RATES];
            
            // Set parameters (from job configuration)
            k = context.getConfiguration().getInt("k_param", UtilityConstants.DEFAULT_K);
            for (int i = 0; i < UtilityConstants.NUM_OF_RATES; ++i)
                m[i] = context.getConfiguration().getInt("m_param_rate_"+(i+1), UtilityConstants.DEFAULT_M);
        }

        @Override
        public void reduce(IntWritable key, Iterable<BloomFilter> values, final Context context)
                throws IOException, InterruptedException 
        {
            int rating = key.get();
            BloomFilter bloomFilter = new BloomFilter(rating, m[rating-1], k, -1);
            
            logger.info(String.format("BloomFilter[%d]: <M = %d K = %d>", rating, m[rating-1], k));

            for (BloomFilter value : values) {
                bloomFilter.or(value);
            }

            context.write(key, bloomFilter);
        }
    }

    public static class MapRedBloomFilterMapperWithIndexes extends Mapper<Object, Text, IntWritable, ArrayPrimitiveWritable> 
    {
        // Number of hash functions to be computed
        private static int k;
        // Vector containg the dimensions in bit of each BloomFilter
        private static final int[] m = new int[UtilityConstants.NUM_OF_RATES];

        //Reuse writable key and value obj
        private static final IntWritable rate = new IntWritable();
        private static final ArrayPrimitiveWritable hashesValue = new ArrayPrimitiveWritable();

        public void setup(Context context) throws IOException, InterruptedException
        {
            // Set k and m BloomFilter parameters (from job configuration)
            k = context.getConfiguration().getInt("k_param", UtilityConstants.DEFAULT_K);
            for (int i = 0; i< UtilityConstants.NUM_OF_RATES; ++i)
                m[i] = context.getConfiguration().getInt("m_param_rate_"+(i+1), UtilityConstants.DEFAULT_M);
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

    public static class MapRedBloomFilterReducerWithIndexes extends Reducer<IntWritable,ArrayPrimitiveWritable,NullWritable,BloomFilter> {

        //TODO add clean for classes
        // Number of hash functions to be computed
        private static int k;
        // Vector containg the dimensions in bit of each BloomFilter
        private static final int[] m = new int[UtilityConstants.NUM_OF_RATES];
        
        // Utility attribute for code readability
        private static final float p = UtilityConstants.FALSE_POSITIVE_RATE;

        private BloomFilter bloomFilter;

        public void setup(Context context) throws IOException, InterruptedException
        {
            // Set k and m BloomFilter parameters (from job configuration)
            k = context.getConfiguration().getInt("k_param", UtilityConstants.DEFAULT_K);
            for (int i = 0; i< UtilityConstants.NUM_OF_RATES; ++i)
                m[i] = context.getConfiguration().getInt("m_param_rate_"+(i+1), UtilityConstants.DEFAULT_M);
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
        
        job.setJarByClass(MapRedBloomFilter.class);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 15) {
           System.err.println("Usage: BloomFilter <input> <output> <num_lines_per_split> <m_value>{10 times} <k_value> <version>");//TODO 10 times or 10 ?
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

        //Set the BloomFilter parameters
       
        //K value
        job.getConfiguration().set("k_param", String.valueOf(k_value));
        
        //M value for each rating
        for(short i = 0; i<UtilityConstants.NUM_OF_RATES; ++i)
            job.getConfiguration().set("m_param_rate_"+(i+1), String.valueOf(m_value[i]));
       
        if (version.equalsIgnoreCase(UtilityConstants.NAME_OF_VERSIONS[0])){ // WithIndexes
            //Set mapper and reducer
            job.setMapperClass(MapRedBloomFilterMapperWithIndexes.class);
            job.setReducerClass(MapRedBloomFilterReducerWithIndexes.class);

            //Define type of mapper's key-value output pair
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(ArrayPrimitiveWritable.class);
            
            //Define type of reducer's key-value output pair
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(BloomFilter.class);
        }
        else { // WithBloomFilters
            
            //Set mapper and reducer
            job.setMapperClass(MapRedBloomFilterMapperWithBloomFilters.class);
            job.setReducerClass(MapRedBloomFilterReducerWithBloomFilters.class);

            //Define type of mapper's key-value output pair
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(BloomFilter.class);
        
            //Define type of reducer's key-value output pair
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(BloomFilter.class);
        }

        //Define number of reduce tasks
        job.setNumReduceTasks(UtilityConstants.NUM_REDUCERS);
        
        // Config input/output
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
       
        NLineInputFormat.setNumLinesPerSplit(job, numLinesPerSplit);     
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));// TODO: args or otherArgs ?

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
