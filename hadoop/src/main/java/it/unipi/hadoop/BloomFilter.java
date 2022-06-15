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
    private BitSet bitArray;

    // TODO: check if this should actually be static
    private final static MurmurHash murmurHash = (MurmurHash) MurmurHash.getInstance();


    // to be used by Shuffle and Sort
    public BloomFilter(){

    }

    /**
     * 
     * @param rating
     * @param m
     * @param K
     * @param p
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
            index %= m;
            index = Math.abs(index);

            // TODO: handle exception
            bitArray.set(index);
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
            index %= m;
            index = Math.abs(index);

            // TODO: handle exception
            if(!bitArray.get(index)){
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
    
    public void clear(){
        this.bitArray.clear();
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeInt(this.rating);
        out.writeInt(this.m);
        out.writeFloat(this.P);
        out.writeInt(this.K);

        byte[] serializedBitSet = this.bitArray.toByteArray();

        // TODO: check if this causes poor memory perfomance  
        out.writeInt(serializedBitSet.length);
        out.write(serializedBitSet);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {

        this.rating = in.readInt();
        this.m = in.readInt();
        this.P = in.readFloat();
        this.K = in.readInt();
        int partialBitsetSize = in.readInt();
        
        byte[] temp = new byte[partialBitsetSize];
        in.readFully(temp);

        // byte arrays are automatically filled with zeros by default upon declaration
        byte[] fullBitSet = new byte[this.m];

        for (int i = 0; i < temp.length; i++) {
            fullBitSet[i] = temp[i];
        }
        
        this.bitArray = BitSet.valueOf(fullBitSet);
    }
    
}
