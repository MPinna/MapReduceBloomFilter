package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;


import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.hash.MurmurHash;

public class BloomFilter implements Writable {

    private int rating;
    private int m;
    private float P;
    private int K;
    private final BitSet bitArray;

    // TODO: check if this should actually be static
    private final static MurmurHash murmurHash = (MurmurHash) MurmurHash.getInstance();

    /**
     * 
     * @param rating
     * @param m
     * @param K
     * @param p2
     */
    public BloomFilter(int rating, int m, int K, float p){
        this.rating = rating;
        this.m = m;
        this.K = K;
        this.P = p;

        this.bitArray = new BitSet(m);
    }
    

    /**
     * 
     * @param movieId
     */
    public void add(String movieId){
        for (int i = 0; i < this.K; i++) {
            int index = murmurHash.hash(movieId.getBytes(), movieId.length(), i);
            //TODO: handle exception
            bitArray.set(index % m);
        }
    }

    /**
     * 
     * @param movieId
     * @return
     */
    public boolean test(String movieId){
        for (int i = 0; i < this.K; i++) {
            int index = murmurHash.hash(movieId.getBytes(), movieId.length(), i);
            //TODO: handle exception
            if(!bitArray.get(index % m)){
                return false;
            }
        }

        return true;
    }

    /**
     * Set to 1 the bit in the bit vector at the
     * specified index
     * @param index index of the bit vector to be set to 1
     */
    //TODO: check index bounds
    public void setAt(int index){
        //TODO: check exception handling
        this.bitArray.set(index);
    }

    public void or(BloomFilter that){
        this.bitArray.or(that.bitArray);
    } 
    
    
    @Override
    public void readFields(DataInput in) throws IOException {

        this.rating = in.readInt();
        this.m = in.readInt();
        this.P = in.readFloat();
        this.K = in.readInt();

        byte[] temp = new byte[this.m];
        in.readFully(temp);
        // TODO: check exception handling of streams etc.
        this.bitArray.or(BitSet.valueOf(temp));        
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeInt(this.rating);
        out.writeInt(this.m);
        out.writeFloat(this.P);
        out.writeInt(this.K);

        // TODO: check if this causes poor memory perfomance  
        out.write(this.bitArray.toByteArray());
    }
    
}
