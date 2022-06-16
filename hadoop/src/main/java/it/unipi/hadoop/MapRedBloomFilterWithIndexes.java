package it.unipi.hadoop;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.util.Progressable;

public class MapRedBloomFilterWithIndexes
{
                                                        
    public static class MapRedBloomFilterMapper extends Mapper<Object, Text, IntWritable, ArrayPrimitiveWritable> 
    {
        // Number of hash functions to be computed
        private static int k;
        private static int m;

        //Reuse writable key and value obj
        private static final IntWritable rate = new IntWritable();
        private static final ArrayPrimitiveWritable hashesValue = new ArrayPrimitiveWritable();

        public void setup(Context context) throws IOException, InterruptedException
        {
            // Set parameters (from job configiguration)
            k =  context.getConfiguration().getInt("k_param", 5);
            m =  context.getConfiguration().getInt("m_param", 1000000);
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
                        
                        // Compute k hash functions
                        int[] hashValue = BloomFilter.computeHash(k, movieId, m);

                        // Compute rounded rating and set Map key
                        rate.set(Math.round(rawRate));

                        // Set Map value
                        hashesValue.set(hashValue);

                        // Emit rating e values of the k hash functions
                        context.write(rate, hashesValue);
                    }
        }
    }

    public static class MapRedBloomFilterReducer extends Reducer<IntWritable,ArrayPrimitiveWritable,NullWritable,BloomFilter> {

        //TODO add clean for classes
        private int k;
        private int m;
        private float p;

        BloomFilter bloomFilter;

        public void setup(Context context) throws IOException, InterruptedException
        {
            // Set parameters (from job configiguration)
            k = context.getConfiguration().getInt("k_param", 5);
            m = context.getConfiguration().getInt("m_param", 1000000);
            p = context.getConfiguration().getFloat("p_param", (float) 0.001);

        }

        public void reduce(final IntWritable key, final Iterable<ArrayPrimitiveWritable> values, final Context context)
                throws IOException, InterruptedException {
                    //Create BloomFilter for the given rating
                    int rating = key.get();
                    bloomFilter = new BloomFilter(rating, m, k, p);
                   
                    //Add indexes
                    for(ArrayPrimitiveWritable value: values){
                        int[] indexes = (int[]) value.get();

                        for(int i=0; i<indexes.length; i++){
                            bloomFilter.setAt(indexes[i]);
                        }
                    }

                    context.write(null, bloomFilter);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            //Save bloomFilter in HDFS
            byte[] bytes = bloomFilter.getBitArray().toByteArray();
            Configuration configuration = new Configuration();
            String defaultFSNode = "hdfs://localhost:9000";
            configuration.set("fs.defaultFS", defaultFSNode);
            FileSystem fileSystem = FileSystem.get(configuration);
            String fileName = "example.txt";
            Path hdfsWritePath = new Path("/data/" + fileName);
            FSDataOutputStream out = fileSystem.create(hdfsWritePath);
            out.write(bytes);
            out.close();
            fileSystem.close();
        }
    }

    public static void main(final String[] args) throws Exception {


        final Configuration conf = new Configuration();
        final Job job = new Job(conf, "BloomFilter");
        job.setJarByClass(MapRedBloomFilterWithIndexes.class);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
           System.err.println("Usage: BloomFilter <input> <output>");
           System.exit(1);
        }
        System.out.println("args[0]: <input>="  + otherArgs[0]);
        System.out.println("args[1]: <output>=" + otherArgs[1]);

        //TODO To change
        //Set the BloomFilter parameters
        job.getConfiguration().set("k_param", "5");
        job.getConfiguration().set("m_param", "1000000");
        job.getConfiguration().set("p_param", "0.001");


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
        job.setNumReduceTasks(1);
        
        //Define I/O
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        //TODO to check
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        NLineInputFormat.setNumLinesPerSplit(job, 500);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
