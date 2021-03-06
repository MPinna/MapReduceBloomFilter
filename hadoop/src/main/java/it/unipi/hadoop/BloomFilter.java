package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.hash.MurmurHash;

import org.json.JSONObject;
import org.json.JSONException;

public class BloomFilter implements Writable {

    private int rating;
    private int m;
    private int K;
    private float P;
    private BitSet bitArray;

    private final static MurmurHash murmurHash = (MurmurHash) MurmurHash.getInstance();


    // to be used by Shuffle and Sort
    public BloomFilter(){}

    /**
     * Create a BloomFilter object from a JSON string
     * @param BloomFilterJsonString
     */
    public BloomFilter(String jsonString) throws JSONException,DecoderException{
        JSONObject jsonObject = new JSONObject(jsonString);
             
        this.rating = jsonObject.getInt("rating");
        this.m = jsonObject.getInt("m");
        this.K = jsonObject.getInt("K");
        this.P = jsonObject.getFloat("P");
        
        String bitArrayString = new String(jsonObject.getString("bitArray"));
        
        byte[] bytes = Hex.decodeHex(bitArrayString.toCharArray());
        this.bitArray = BitSet.valueOf(bytes);
    }

    /**
     * Create a new BloomFilter object
     * @param rating a number from 1 to 10 associated with the bloom filter
     * @param m length of the bit vector
     * @param K number of hash functions to be used
     * @param p probability of false positives
     */
    public BloomFilter(int rating, int m, int K, float p){
        this.rating = rating;
        this.m = m;
        this.K = K;
        this.P = p;

        this.bitArray = new BitSet(m);
    }

    public BitSet getBitArray(){
        return this.bitArray;
    }

    /**
     * Add a movie to the Bloom Filter
     * @param movieId a string from the IMDB collection with the ID of a movie
     */
    public void add(String movieId){
        int[] hashIndexes = computeHash(this.K, movieId, this.m);
        // Check indexes validity
        for (int i = 0; i < this.K; i++) {
            if (hashIndexes[i] < 0 || hashIndexes[i] >= m)
                return ;
        }
        // Set bits at the computed indexes
        for (int i = 0; i < this.K; i++) {
            this.bitArray.set(hashIndexes[i]);
        }
    }

    /**
     * Test if a movie is present in the filter
     * @param movieId  a string from the IMDB collection with the ID of a movie
     * @return true if the movie might be in the filter
     *          false if the movie is definitely not in the filter
     */
    public boolean test(String movieId){
        int[] hashIndexes = computeHash(this.K, movieId, this.m);
        for (int i = 0; i < this.K; i++) {
            int index = hashIndexes[i];
            if (index < 0 || index >= m)
                return false;
            if(!this.bitArray.get(hashIndexes[i])){
                return false;
            }
        }
        return true;
    }

    /**
     * Compute the outputs of all the hash functions
     * @param K Number of hash functions
     * @param movieId id of the movie whose digests have to be computed
     * @param m length of the bit vector of  the filter
     * @return
     */
    public static int[] computeHash (int K, String movieId, int m){
        int[] hashIndexes = new int[K];
        for (int i = 0; i < K; i++) {
            hashIndexes[i] = murmurHash.hash(movieId.getBytes(), movieId.length(), i);
            hashIndexes[i] %= m;
            hashIndexes[i] = Math.abs(hashIndexes[i]);
        }
        return hashIndexes;
    }

    /**
     * Set to 1 the bit in the bit vector at the
     * specified index
     * @param index index of the bit vector to be set to 1
     */
    public void setAt(int index){
        //Check index bounds
        if(index >=0 && index < this.m)
            this.bitArray.set(index);
    }

    /**
     * Perform a bit-wise OR between the object and another bloom filter
     * @param that the bloom filter whose bitarray is to be OR-ed with the object
     */
    public void or(BloomFilter that){
        this.bitArray.or(that.bitArray);
    } 
    
    /**
     * Reset all the bits of the bloom filter to 0
     */
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
    
    @Override
    public String toString(){

        final JSONObject jsonObject = new JSONObject();
        try{
            jsonObject.put("rating", this.rating);
            jsonObject.put("m", this.m);
            jsonObject.put("K", this.K);
            jsonObject.put("P", this.P);
        }
        catch(JSONException e){
            return "JSONException";
        }
        final StringBuilder builder = new StringBuilder();
        
        for(byte b : bitArray.toByteArray()) {
        builder.append(String.format("%02x", b));
        }

        jsonObject.put("bitArray", builder.toString());
        return jsonObject.toString();
    }

    public int getRating(){
        return rating;
    }

}
