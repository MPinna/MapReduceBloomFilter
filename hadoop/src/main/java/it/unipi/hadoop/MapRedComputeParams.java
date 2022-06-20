package it.unipi.hadoop;
import java.io.IOException;
import org.apache.log4j.Logger; 

import it.unipi.hadoop.Util.UtilityConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapRedComputeParams
{
    public static class MapRedComputeParamsMapper extends Mapper<Object, Text, IntWritable, IntWritable>
    {
        private static final int[] ratings_count = new int[UtilityConstants.NUM_OF_RATES];
        private static final Logger logger = Logger.getLogger(MapRedComputeParamsMapper.class.getName());
        private static final IntWritable outKey = new IntWritable();
        private static final IntWritable outValue = new IntWritable();
   
        @Override
        public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException 
        {
            Object[] tokens = Util.parseInput(value.toString());  
            if (tokens == null){
                logger.info(String.format("Invalid entry: %s", value.toString()));
                return;  
            }
            
            ratings_count[(int)tokens[1]-1]++; //TODO: is it nice ?   
                    
        }

        /*
         * The 10 values are emitted just once at the end of map task
         */
        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (int i = 0; i < UtilityConstants.NUM_OF_RATES; ++i){
                outKey.set(i + 1);
                outValue.set(ratings_count[i]);
                
                context.write(outKey, outValue);
            }
        }

    }
    
    public static class MapRedComputeParamsReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        private static Text outValue = new Text();
        private static int bestM;
        private static int bestK;
        private static int maxK;
        private static int N;
        private static float P;
        private static final Logger logger = Logger.getLogger(MapRedComputeParamsReducer.class.getName());
       
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            P =  context.getConfiguration().getFloat("P", UtilityConstants.FALSE_POSITIVE_RATE);
            maxK = context.getConfiguration().getInt("maxK", Integer.MAX_VALUE);
        }
       
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, final Context context)
                throws IOException, InterruptedException 
        {
            
            N = 0;
                     
            for (IntWritable value : values) {
                N += value.get();
            }
            
            bestM = (int)Math.ceil((N * Math.log(P)) / Math.log(1 / Math.pow(2, Math.log(2))));
            bestK = (int)Math.round(Math.log(2)*bestM/N);
            
            if (bestK > maxK){
                bestK = maxK;
                bestM = (int)Math.ceil(-(maxK*N)/Math.log(1-Math.pow(P, 1.0f/maxK)));
            }
            
            outValue.set(String.format("%d\t%d\t%d",N, bestM, bestK));
            
            logger.info(String.format("Rating: %d, N: %d, bestM: %d, bestK: %d", key.get(), N, bestM, bestK));
            
            context.write(key, outValue);     
        }
    }

    public static void main(final String[] args) throws Exception {

        final Configuration conf = new Configuration();
        final Job job = new Job(conf, "ComputeParams");
        
        job.setJarByClass(MapRedComputeParams.class);

        // Read job parameters 
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 4) {
           System.err.println("Usage: BloomFilter <input> <output> <false_positive_rate> <num_lines_per_split> [maxK]");
           System.exit(1);
        }

        System.out.println("args[0]: <input>="  + otherArgs[0]);
        System.out.println("args[1]: <output>=" + otherArgs[1]);
        System.out.println("args[2]: <false_positive_rate>=" + otherArgs[2]);
        System.out.println("args[3]: <num_lines_per_split>=" + otherArgs[3]);
           
        int numLinesPerSplit = -1;
        int maxK = Integer.MAX_VALUE;
        // Check inputs args
        try{
            float p = Float.parseFloat(otherArgs[2]);
            numLinesPerSplit = Integer.parseInt(otherArgs[3]);
            if(otherArgs.length == 5){
                System.out.println("args[4]: <maxK>=" + otherArgs[4]);
                maxK = Integer.parseInt(otherArgs[4]);
            }
            if ((p <= 0 || p > 1) || (numLinesPerSplit < 1) || (maxK < 1)){
                throw new NumberFormatException();
            }
        }
        catch(NumberFormatException ex){
            System.err.println("Usage: BloomFilter <input> <output> <false_positive_rate> <num_lines_per_split> [maxK]");
            return;
        }
        
        //Set job parameters
        job.getConfiguration().set("P", otherArgs[2]);
        job.getConfiguration().set("maxK", String.valueOf(maxK));

        
        // Config mapper 
        job.setMapperClass(MapRedComputeParamsMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
    
        //Config reducer
        job.setNumReduceTasks(UtilityConstants.NUM_REDUCERS);
        job.setReducerClass(MapRedComputeParamsReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
       
        // Config input/output
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job, numLinesPerSplit);     

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
