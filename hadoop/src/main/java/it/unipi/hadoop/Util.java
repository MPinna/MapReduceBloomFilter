package it.unipi.hadoop;

public class Util {
    public final static class UtilityConstants{
        //Constant value for number of possible different rounded rates
        public static final int NUM_OF_RATES = 10;
    
        //Constant value for the maximum allowed value for rate
        public static final int MAX_RATE = 10;

        //Constant value for the minimum allowed value for rate
        public static final int MIN_RATE = 1;

        //Constant value for BloomFilter required false positive rate
        public static final float FALSE_POSITIVE_RATE = (float) 0.01; //1%

        //Constant value for items count per rate
        public static final int DEFAULT_COUNT_PER_RATE = 1; 
        
        //Constant values for implementation versions name
        public static final String[] NAME_OF_VERSIONS = {"WithIndexes","WithBloomFilters"};    
        
        //Constant defualt values for K and M BloomFilter parameters
        public static final int DEFAULT_K= 5;
        public static final int DEFAULT_M = 40960000; // 5 MiB

        //Constant value for number of reduce tasks
        public static final int NUM_REDUCERS = 1;
    }

    public static final Object[] parseInput(String value){
        Object[] out = new Object[2];

        // Extract raw input values
        String tokens[] = value.split("\t+");
            
        // Check format (last value is discarded)
        if (tokens.length != 3) {
            return null;
        }

        String movieId = null;
        Integer index = -1;
        
        // Get movie id
        movieId = tokens[0];
        if(movieId == null){
            return null;
        }

        // Get rating
        try{
            float rating = Float.parseFloat(tokens[ 1]); 
            // Compute rounded rate
            index = Math.round(rating);
            if (index < UtilityConstants.MIN_RATE || index > (float) UtilityConstants.MAX_RATE){
                return null;
            }
        }
        catch(NumberFormatException e){
            return null;
        }
        
        out[0] = movieId;
        out[1] = index; 
        
        return out;
    }
}
