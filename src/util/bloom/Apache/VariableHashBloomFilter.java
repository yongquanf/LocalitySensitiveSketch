package util.bloom.Apache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import util.bloom.Apache.Hash.Hash;

//import util.bloom.Apache.Hash.Hash;
import edu.harvard.syrah.prp.Log;

public class VariableHashBloomFilter extends Filter {
		 
		Log log =new Log(VariableHashBloomFilter.class);
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 4571691937049250188L;

	private static final byte[] bitvalues = new byte[] {
	    (byte)0x01,
	    (byte)0x02,
	    (byte)0x04,
	    (byte)0x08,
	    (byte)0x10,
	    (byte)0x20,
	    (byte)0x40,
	    (byte)0x80
	  };
	  
	  /** The bit vector. */
	  BitSet bits=null;

	  
//	  /**
//	   * index of the hash function (3d hash function)
//	   * (level,indedx,Indhash)
//	   */
//	  HashFuncSeeds[]HashIndex;
//	  
//	  
//	    class HashFuncSeeds{
//	    	int level;
//	    	int index;
//	    	int IndHash;
//	    	
//	    	HashFuncSeeds(int a_,int b_,int c_){
//	    		 level=a_;
//	    		index=b_;
//	    		IndHash=c_;
//	    	}
//	    }
	  
	  /**
	   * number of keys inserted
	   */
	  public int NumInserted=0;
	  
	  /** Default constructor - use with readFields */
	  public VariableHashBloomFilter() {
	    super();
	    NumInserted=0;
	    indexHashGroup=-1;
	    
	  }

	  
	  /**
	   * Constructor
	   * @param vectorSize The vector size of <i>this</i> filter.
	   * @param nbHash The number of hash function to consider.
	   * @param hashType type of the hashing function (see
	   * {@link org.apache.hadoop.util.hash.Hash}).
	   */
	  public VariableHashBloomFilter(int vectorSize_, int nbHash_, int hashType_
			  ,int level) {
	    super(vectorSize_, nbHash_, hashType_,level);
		    
		  
		this.bits = new BitSet(this.vectorSize);
		this.bits.clear();
		
		this.NumInserted=0;
		this.indexHashGroup=level;
	   
	   }

	  /**
	   * create a Bloom Filter using a BitSet
	   * @param mm
	   * @return
	   */
	  public VariableHashBloomFilter createBloomFilter(BitSet mm) {
		// TODO Auto-generated constructor stub
		  VariableHashBloomFilter f=new VariableHashBloomFilter(vectorSize,nbHash,hashType
				  ,indexHashGroup);
		  if(mm!=null){
			  f.NumInserted=-1;
			  f.bits.or(mm);
		  }
		  return f;	  
	}

	@Override
	  public void add(Key key) {
	    if(key == null) {
	      throw new NullPointerException("key cannot be null");
	    }

	    int[] h = hash.hash(key);
	    //test if we add a new key
	  if(true){
	    boolean isAllTrue=true;
	    for(int i = 0; i < nbHash; i++) {  
	    	if(!bits.get(h[i])){
	    		isAllTrue=false;
	    	}
	    }
	    if(!isAllTrue){
	    	NumInserted++;
	    }
	  }
	  //NumInserted++;
	  //NumInserted++;
	    //set the key
	    hash.clear();
	    //System.out.print("\n################\n");
	    for(int i = 0; i < nbHash; i++) {     
	      bits.set(h[i]);
	    //  System.out.print(bits.get(h[i])+" ");	
	    }
	  //  System.out.print("\n################\n");
	    
	    
	  }

	  /**
	   * set the bits and return the indexes of the bits
	   * @param key
	   * @return
	   */
	  public int[] addAndReturnIndex(Key key) {
		    if(key == null) {
		      throw new NullPointerException("key cannot be null");
		    }

		    int[] h = hash.hash(key);
		    
		    boolean isAllTrue=true;
		    for(int i = 0; i < nbHash; i++) {  
		    	if(!bits.get(h[i])){
		    		isAllTrue=false;
		    	}
		    }
		    if(!isAllTrue){
		    	NumInserted++;
		    }
		    
		    hash.clear();
		    //System.out.print("\n################\n");
		    for(int i = 0; i < nbHash; i++) {     
		      bits.set(h[i]);
		    //  System.out.print(bits.get(h[i])+" ");	
		    }
		    return h;
		  //  System.out.print("\n################\n");
		  }
	  
	  @Override
	  public void and(Filter filter) {
	    if(filter == null
	        || !(filter instanceof VariableHashBloomFilter)
	        || filter.vectorSize != this.vectorSize
	        || filter.nbHash != this.nbHash) {
	      throw new IllegalArgumentException("filters cannot be and-ed");
	    }

	    this.bits.and(((VariableHashBloomFilter) filter).bits);
	  }

	  @Override
	  public boolean membershipTest(Key key) {
	    if(key == null) {
	      throw new NullPointerException("key cannot be null");
	    }

	    int[] h = hash.hash(key);
	    hash.clear();
	    for(int i = 0; i < nbHash; i++) {
	    	//System.out.println("$: "+h[i]+", "+bits.get(h[i]));
	      if(!bits.get(h[i])) {
	        return false;
	      }
	    }
	    return true;
	  }

	  /**
	   * get the hashed Key
	   * @param key
	   * @return
	   */
	  public int[] getHash(Key key){
		  return hash.hash(key);
	  }
	  
	  /**
	   * test membership and return the index
	   * @param key
	   * @param Indexes
	   * @return
	   */
	  public boolean membershipTestAndReturnIndexes(Key key) {
		  
		  	//intialize as null
		  	
		  
		    if(key == null) {
		      throw new NullPointerException("key cannot be null");
		    }

		    int[] h = hash.hash(key);
		    hash.clear();
		    for(int i = 0; i < nbHash; i++) {
		    	//System.out.println("$: "+h[i]+", "+bits.get(h[i]));
		      if(!bits.get(h[i])) {
		        return false;
		      }
		    }
		    //System.out.println("$: Yes, found!");
		    //yes
		    
		    return true;
		  }
	  
	  /**
	   * true bits
	   * @return
	   */
	  	public int getNumTrueBits(){
		  int sum=0;
		  //return bits.cardinality();
		  
		  if(bits.cardinality()>0){
			  for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i+1)) {
				  if(i>=0){
					  sum++;
				  }
			  }
			  }
		  return sum;
	  }
	  
	  /**
	   * get all true bits
	   * @return
	   */
	  public List<Integer> getAllTrueBits(){
		  List<Integer> indexes=new ArrayList<Integer>(2);
		  /*for(int i=0;i<vectorSize;i++){
			  if(bits.get(i)){
				  indexes.add(i);
			  }
		  }*/
		  //has true bits
		  if(bits.cardinality()>0){
		  for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i+1)) {
			  if(i>=0){
				  indexes.add(i);
			  }
		  }
		  }
		  return indexes;
	  }
	  
	  /**
	   * reset the fit
	   * @param pos
	   */
	  public void flipBit(int pos){
		  bits.flip(pos);
	  }
	  
	  @Override
	  public void not() {
	    bits.flip(0, vectorSize - 1);
	  }

	  @Override
	  public void or(Filter filter) {
	    if(filter == null
	        || !(filter instanceof VariableHashBloomFilter)
	        || filter.vectorSize != this.vectorSize
	        || filter.nbHash != this.nbHash) {
	      throw new IllegalArgumentException("filters cannot be or-ed");
	    }
	    bits.or(((VariableHashBloomFilter) filter).bits);
	  }

	  @Override
	  public void xor(Filter filter) {
	    if(filter == null
	        || !(filter instanceof VariableHashBloomFilter)
	        || filter.vectorSize != this.vectorSize
	        || filter.nbHash != this.nbHash) {
	      throw new IllegalArgumentException("filters cannot be xor-ed");
	    }
	    bits.xor(((VariableHashBloomFilter) filter).bits);
	  }

	  @Override
	  public String toString() {
	    return bits.toString();
	  }

	  /**
	   * get a copy of the filter
	   * @return
	   */
	  public VariableHashBloomFilter getCopy(){
		  VariableHashBloomFilter f=new VariableHashBloomFilter(
				  vectorSize,nbHash,hashType,indexHashGroup);
		  f.or(this);
		  
		  f.NumInserted=this.NumInserted;
		  f.indexHashGroup=this.indexHashGroup;
		  
		  return f;
	  }
	  
	  public BitSet getBitSet(){
		  return bits;
		  
	  }
	  
	  /**
	   * @return size of the the VariableHashBloomFilter
	   */
	  public int getVectorSize() {
	    return vectorSize;
	  }

	  // Writable

	  @Override
	  public void write(DataOutput out) throws IOException {
	    super.write(out);
	    byte[] bytes = new byte[getNBytes()];
	    for(int i = 0, byteIndex = 0, bitIndex = 0; i < getVectorSize(); i++, bitIndex++) {
	      if (bitIndex == 8) {
	        bitIndex = 0;
	        byteIndex++;
	      }
	      if (bitIndex == 0) {
	        bytes[byteIndex] = 0;
	      }
	      if (bits.get(i)) {
	        bytes[byteIndex] |= bitvalues[bitIndex];
	      }
	    }
	    out.write(bytes);
	  }

	  @Override
	  public void readFields(DataInput in) throws IOException {
	    super.readFields(in);
	    bits = new BitSet(this.getVectorSize());
	    byte[] bytes = new byte[getNBytes()];
	    in.readFully(bytes);
	    for(int i = 0, byteIndex = 0, bitIndex = 0; i < getVectorSize(); i++, bitIndex++) {
	      if (bitIndex == 8) {
	        bitIndex = 0;
	        byteIndex++;
	      }
	      if ((bytes[byteIndex] & bitvalues[bitIndex]) != 0) {
	        bits.set(i);
	      }
	    }
	  }
	  
	  /* @return number of bytes needed to hold bit vector */
	  private int getNBytes() {
	    return (getVectorSize() + 7) / 8;
	  }
	  
	  /**
	   * reset to all false bits
	   */
	  public void clear(){
		 if(this.bits!=null){
			 this.bits.clear();
		 }
	  }
	  /**
	   * get the size of the bits
	   * @return
	   */
	  public int getSize(){
		  return getVectorSize();
	  }
	  
	  public static void main(String[] args){
		  
		  int level=1;
		  VariableHashBloomFilter test=new VariableHashBloomFilter(1024*4,8,Hash.MURMUR_HASH,
				  level);
		  
		  
		  
		  
		  

		  
		  Vector<Key> vec=new Vector<Key>(1);
		  Random r =new Random(1);	
		  
		  
		  int size=0;
		  
		  
		  for(int j=0;j<500;j++){
			  
			  byte[] key=new byte[6];;
			  	  
			  r.nextBytes( key );
			  
			  Key k=new Key(key);
			  
			  vec.add(k.makeCopy());		  
			  if(j%2==0){
				  test.add(k.makeCopy());
				  size++;			 
			  }
		  }
		  
		  int count=0;
		  for(int j=0;j<vec.size();j++){
			  Key k=vec.get(j);
			  //System.out.println("$: "+POut.toString(k.getBytes()));
			  
			  if(test.membershipTest(k)){
				  count++;
				//  System.out.println("======================");
			  }
		  }
		  
		  System.out.println("$: size: "+vec.size());
		  
		  System.out.println("$: Total: "+size);
		  
		  System.out.println("$: real: "+count);
		  
		  
	  }
	  
	  /**
	   * set a bit
	   * @param indKey
	   */
	  public void setBit(int indKey) {
		// TODO Auto-generated method stub
		  bits.set(indKey);
		}
	  /**
	   * query the bit
	   * @param indKey
	   * @return
	   */
	  public boolean queryBit(int indKey){
		  if(bits.get(indKey)){
			  return true;
		  }else{
			  return false;
		  }
	  }
	  
	  /**
	   * filled factor
	   * @return
	   */
	  public double filledFactor(){
		 int ff=getAllTrueBits().size();
		 //int  ff=bits.cardinality();
		  
		 return (ff+0.0)/vectorSize;
		 
	  }

	  /**
	   * prior fp
	   * @param n
	   * @return
	   */
	  public double getAveragedPriorFP(int n) {
		// TODO Auto-generated method stub
		  
					//		nbHashLev_2_Bloom);
		  
		return Math.pow((1-Math.exp(-(n*nbHash+0.0)/vectorSize)),nbHash);
	  }
	  
	  	/**
	  	 * prior fp
	  	 * @return
	  	 */
	  	public double getAveragedPriorFP() {
			// TODO Auto-generated method stub
			  
						//		nbHashLev_2_Bloom);
	  		int n=NumInserted;
	  		
			return Math.pow((1-Math.exp(-(n*nbHash+0.0)/vectorSize)),nbHash);
		  }
	  
	  
	  	/**
	  	 * posterior 
	  	 * @return
	  	 */
		public double getAveragedObservedFalsePositive() {
		// TODO Auto-generated method stub
			
			return Math.pow(filledFactor(), nbHash);
		}
		/**
		 * different bits
		 * @param b
		 * @return
		 */
		public int diff(VariableHashBloomFilter b){
			int s=0;
			for(int i=0;i<vectorSize;i++){
				if(queryBit(i)!=b.queryBit(i)){
					s++;
				}			
			}
			return s;
		}
	  
	}//end class
