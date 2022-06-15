package it.unipi.hadoop;

import org.junit.Test;
import static org.junit.Assert.*;

public class BloomFilterTest{
    
    private final int M = 1 << 23; // 1 MB = 2 ^ 20 bit
    private final int K = 3;
    private final float P = 0.001f;

    private final int N_ADD = 1000;
    private final int N_TEST = 1000;

    private BloomFilter bloomFilter;

    @Test  
    public void test() {
        for (int rating = 1; rating < 11; ++rating){
            bloomFilter = new BloomFilter(rating, M, K, P);
            
            for (int i = 0; i < N_ADD; ++i)
                bloomFilter.add(String.format("ID_%d", i));
            
            for (int i = 0; i < N_TEST; ++i)
                assertTrue(bloomFilter.test(String.format("ID_%d", i)));
        }
    }
}