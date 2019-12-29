/**
 *
 * Copyright (c) 2005, European Commission project OneLab under contract 034819 (http://www.one-lab.org)
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or 
 * without modification, are permitted provided that the following 
 * conditions are met:
 *  - Redistributions of source code must retain the above copyright 
 *    notice, this list of conditions and the following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright 
 *    notice, this list of conditions and the following disclaimer in 
 *    the documentation and/or other materials provided with the distribution.
 *  - Neither the name of the University Catholique de Louvain - UCL
 *    nor the names of its contributors may be used to endorse or 
 *    promote products derived from this software without specific prior 
 *    written permission.
 *    
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS 
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE 
 * COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, 
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; 
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN 
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package util.bloom.Apache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import org.pcap4j.util.ByteArrays;

import com.sun.xml.internal.ws.message.ByteArrayAttachment;

import util.bloom.Apache.Hash.Hash;

import edu.harvard.syrah.prp.Log;

/**
 * Implements a <i>counting Bloom filter</i>, as defined by Fan et al. in a ToN
 * 2000 paper.
 * <p>
 * A counting Bloom filter is an improvement to standard a Bloom filter as it
 * allows dynamic additions and deletions of set membership information.  This 
 * is achieved through the use of a counting vector instead of a bit vector.
 * <p>
 * Originally created by
 * <a href="http://www.one-lab.org">European Commission One-Lab Project 034819</a>.
 *
 * @see Filter The general behavior of a filter
 * 
 * @see <a href="http://portal.acm.org/citation.cfm?id=343571.343572">Summary cache: a scalable wide-area web cache sharing protocol</a>
 */
public final class CountingBloomFilter extends Filter {
	
	static Log log =new Log(CountingBloomFilter.class);
	
  /**
	 * 
	 */
	private static final long serialVersionUID = -1793895190901720010L;

/** Storage for the counting buckets */
  //private long[] buckets;

  /** We are using 4bit buckets, so each bucket can count to 15 */
  //private final static long BUCKET_MAX_VALUE = 15;
	
	public int[] values;

  /** Default constructor - use with readFields */
  public CountingBloomFilter() {}
  
  /**
   * Constructor
   * @param vectorSize The vector size of <i>this</i> filter.
   * @param nbHash The number of hash function to consider.
   * @param hashType type of the hashing function (see
   * {@link org.apache.hadoop.util.hash.Hash}).
   */
  public CountingBloomFilter(int vectorSize, int nbHash, int hashType) {
    super(vectorSize, nbHash, hashType);
    
    values=new int[vectorSize];
    for(int i=0;i<values.length;i++){
    	values[i]=0;
    }
    
   /* buckets = new long[buckets2words(vectorSize)];
    for(int i=0;i<buckets2words(vectorSize);i++){
    	buckets[i]=0;
    }*/
  }

  /** returns the number of 64 bit words it would take to hold vectorSize buckets */
  private static int buckets2words(int vectorSize) {
   return ((vectorSize - 1) >>> 4) + 1;
  }

  /**
   * test empty
   * @return
   */
  public boolean isEmpty(){
	  
	  if(values==null){
		  return true;
	  }
	  for(int i=0;i<values.length;i++){
		  if(values[i]!=0){
			  return false;
		  }
	  }
	  /*
	  if(buckets==null){
		  return true;
	  }
	  for(int i=0;i<buckets.length;i++){
		  if(buckets[i]!=0){
			  return false;
		  }
	  }
	  */
	  return true;
  }
  
  @Override
  public void add(Key key) {
    if(key == null) {
      throw new NullPointerException("key can not be null");
    }

    int[] h = hash.hash(key);
    hash.clear();

    for(int i = 0; i < nbHash; i++) {
       values[h[i]]++;
    }
    
    
    /*for(int i = 0; i < nbHash; i++) {
      // find the bucket
      int wordNum = h[i] >> 4;          // div 16
      int bucketShift = (h[i] & 0x0f) << 2;  // (mod 16) * 4
      
      long bucketMask = 15L << bucketShift;
      long bucketValue = (buckets[wordNum] & bucketMask) >>> bucketShift;
      
      // only increment if the count in the bucket is less than BUCKET_MAX_VALUE
      if(bucketValue < BUCKET_MAX_VALUE) {
        // increment by 1
        buckets[wordNum] = (buckets[wordNum] & ~bucketMask) | ((bucketValue + 1) << bucketShift);
      }
    }*/
    
  }

  /**
   * set the bits
   * @param h
   */
  public void setBits(int[] h){
	  
	  for(int i = 0; i < nbHash; i++) {
	       values[h[i]]++;
	    }
  }
  //test the bits
  public void deleteBits(int[] h){	  
	  for(int i = 0; i < nbHash; i++) {
	       values[h[i]]--;
	    }
  }
  
  /**
   * is zero
   * @param idx
   * @return
   */
  public boolean isZero(int idx){
	  if(values[idx]==0){
		  return true;
	  }else{
		  return false;
	  }
  }
  /**
   * set the bit
   * @param idx
   */
  	public void setBit(int idx){
	  
  		
  		values[idx]++;
  		 // find the bucket
        /*int wordNum = idx >> 4;          // div 16
        int bucketShift = (idx & 0x0f) << 2;  // (mod 16) * 4
        
        long bucketMask = 15L << bucketShift;
        long bucketValue = (buckets[wordNum] & bucketMask) >>> bucketShift;
        
        // only increment if the count in the bucket is less than BUCKET_MAX_VALUE
        if(bucketValue < BUCKET_MAX_VALUE) {
          // increment by 1
          buckets[wordNum] = (buckets[wordNum] & ~bucketMask) | ((bucketValue + 1) << bucketShift);
        }*/
  		
  	}
  	
  	/**
  	 * subtract 1
  	 * @param idx
  	 */
  	public void deleteBit(int idx){
  		 // find the bucket
  		
  		values[idx]--;
        /*int wordNum = idx >> 4;          // div 16
        int bucketShift = (idx & 0x0f) << 2;  // (mod 16) * 4
        
        long bucketMask = 15L << bucketShift;
        long bucketValue = (buckets[wordNum] & bucketMask) >>> bucketShift;
        
        // only decrement if the count in the bucket is between 0 and BUCKET_MAX_VALUE
        if(bucketValue >= 1 && bucketValue < BUCKET_MAX_VALUE) {
          // decrement by 1
          buckets[wordNum] = (buckets[wordNum] & ~bucketMask) | ((bucketValue - 1) << bucketShift);
        }*/
  	}
  
  	/**
  	 * posterior
  	 * @return
  	 */
  	public double getPosteriorFP(){  		
  		return filledFactor();
  	
  	}
  	
  	/**
  	 * # ones
  	 * @return
  	 */
  	public int NonZeros(){
  		int sum=0;
  		for(int i=0;i<values.length;i++){
  			if(values[i]>0){
  				sum++;
  			}
  		}
  		return sum;
  		
  	}
  	/**
  	 * compute the filled factor
  	 * @return
  	 */
  	public double filledFactor(){
  		
  		int sum=0;
  		for(int i=0;i<values.length;i++){
  			if(values[i]>0){
  				sum++;
  			}else if(values[i]<0){
  				log.error("false negative");
  			}
  		}
  		
  		return (sum+0.0)/values.length;
  		
  		
  	  /*int s=0;
  	  for(int i = 0; i < vectorSize; i++) {
  	      // find the bucket
  	      int wordNum = i >> 4;          // div 16
  	      int bucketShift = (i & 0x0f) << 2;  // (mod 16) * 4

  	      long bucketMask = 15L << bucketShift;

  	      if((buckets[wordNum] & bucketMask) ==0) {
  	        s++;
  	      }
  	    }
  	  
  	  //s=s/4;
  	  int nn=vectorSize;
  	  
  	  //log.info(s+", "+nn);
  	  
  	  return (nn-s+0.0)/nn;
  	  */
  	}
  	
  /**
   * Removes a specified key from <i>this</i> counting Bloom filter.
   * <p>
   * <b>Invariant</b>: nothing happens if the specified key does not belong to <i>this</i> counter Bloom filter.
   * @param key The key to remove.
   */
  public void delete(Key key) {
    if(key == null) {
      throw new NullPointerException("Key may not be null");
    }
    if(!membershipTest(key)) {
      throw new IllegalArgumentException("Key is not a member");
    }

    int[] h = hash.hash(key);
    hash.clear();

    for(int i = 0; i < nbHash; i++) {
    	values[h[i]]--;
    	    	
    }
    
    /*for(int i = 0; i < nbHash; i++) {
      // find the bucket
      int wordNum = h[i] >> 4;          // div 16
      int bucketShift = (h[i] & 0x0f) << 2;  // (mod 16) * 4
      
      long bucketMask = 15L << bucketShift;
      long bucketValue = (buckets[wordNum] & bucketMask) >>> bucketShift;
      
      // only decrement if the count in the bucket is between 0 and BUCKET_MAX_VALUE
      if(bucketValue >= 1 && bucketValue < BUCKET_MAX_VALUE) {
        // decrement by 1
        buckets[wordNum] = (buckets[wordNum] & ~bucketMask) | ((bucketValue - 1) << bucketShift);
      }
    }*/
  }

  @Override
  public void and(Filter filter) {
    if(filter == null
        || !(filter instanceof CountingBloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new IllegalArgumentException("filters cannot be and-ed");
    }
    CountingBloomFilter cbf = (CountingBloomFilter)filter;
    
    for(int i=0;i<values.length;i++){
    	this.values[i]&=cbf.values[i];
    }
    
    /*
    int sizeInWords = buckets2words(vectorSize);
    for(int i = 0; i < sizeInWords; i++) {
      this.buckets[i] &= cbf.buckets[i];
    }*/
  }

  @Override
  public boolean membershipTest(Key key) {
    if(key == null) {
      throw new NullPointerException("Key may not be null");
    }

    int[] h = hash.hash(key);
    hash.clear();

    for(int i = 0; i < nbHash; i++) {
    	
    	if(values[h[i]]==0){
    		return false;
    	}
    }
    
    /*for(int i = 0; i < nbHash; i++) {
      // find the bucket
      int wordNum = h[i] >> 4;          // div 16
      int bucketShift = (h[i] & 0x0f) << 2;  // (mod 16) * 4

      long bucketMask = 15L << bucketShift;

      if((buckets[wordNum] & bucketMask) == 0) {
        return false;
      }
    }*/

    return true;
  }

  /**
   * This method calculates an approximate count of the key, i.e. how many
   * times the key was added to the filter. This allows the filter to be
   * used as an approximate <code>key -&gt; count</code> map.
   * <p>NOTE: due to the bucket size of this filter, inserting the same
   * key more than 15 times will cause an overflow at all filter positions
   * associated with this key, and it will significantly increase the error
   * rate for this and other keys. For this reason the filter can only be
   * used to store small count values <code>0 &lt;= N &lt;&lt; 15</code>.
   * @param key key to be tested
   * @return 0 if the key is not present. Otherwise, a positive value v will
   * be returned such that <code>v == count</code> with probability equal to the
   * error rate of this filter, and <code>v &gt; count</code> otherwise.
   * Additionally, if the filter experienced an underflow as a result of
   * {@link #delete(Key)} operation, the return value may be lower than the
   * <code>count</code> with the probability of the false negative rate of such
   * filter.
   */
  public int approximateCount(Key key) {
    int res = Integer.MAX_VALUE;
    int[] h = hash.hash(key);
    hash.clear();
    
    
    /*for (int i = 0; i < nbHash; i++) {
      // find the bucket
      int wordNum = h[i] >> 4;          // div 16
      int bucketShift = (h[i] & 0x0f) << 2;  // (mod 16) * 4
      
      long bucketMask = 15L << bucketShift;
      long bucketValue = (buckets[wordNum] & bucketMask) >>> bucketShift;
      if (bucketValue < res) res = (int)bucketValue;
    }*/
    if (res != Integer.MAX_VALUE) {
      return res;
    } else {
      return 0;
    }
  }

  /**
   * get a copy
   * @return
   */
  public CountingBloomFilter getCopy(){
	  CountingBloomFilter f=new CountingBloomFilter(vectorSize,nbHash,hashType);
	  
	  for(int i=0;i<values.length;i++){
		  f.values[i]=this.values[i];
	  }
	  
	  /*for(int i=0;i<buckets.length;i++){
	    	f.buckets[i]=buckets[i];
	    }*/
	  return f;
  }
  
  @Override
  public void not() {
    throw new UnsupportedOperationException("not() is undefined for "
        + this.getClass().getName());
  }

  @Override
  public void or(Filter filter) {
    if(filter == null
        || !(filter instanceof CountingBloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new IllegalArgumentException("filters cannot be or-ed");
    }

    CountingBloomFilter cbf = (CountingBloomFilter)filter;

    for(int i=0;i<values.length;i++){
    	this.values[i]|=cbf.values[i];
    }
    
    /*
    int sizeInWords = buckets2words(vectorSize);
    for(int i = 0; i < sizeInWords; i++) {
      this.buckets[i] |= cbf.buckets[i];
    }*/
  }

  @Override
  public void xor(Filter filter) {
    throw new UnsupportedOperationException("xor() is undefined for "
        + this.getClass().getName());
  }

  @Override
  public String toString() {
    StringBuilder res = new StringBuilder();

    for(int i = 0; i < vectorSize; i++) {
      if(i > 0) {
        res.append(" ");
      }
      
      int wordNum = i >> 4;          // div 16
      int bucketShift = (i & 0x0f) << 2;  // (mod 16) * 4
      
      long bucketMask = 15L << bucketShift;
      //long bucketValue = (buckets[wordNum] & bucketMask) >>> bucketShift;
      
      //res.append(bucketValue);
    }

    return res.toString();
  }

  // Writable

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
 /*   int sizeInWords = buckets2words(vectorSize);
    for(int i = 0; i < sizeInWords; i++) {
      out.writeLong(buckets[i]);
    }*/
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
   /* int sizeInWords = buckets2words(vectorSize);
    buckets = new long[sizeInWords];
    for(int i = 0; i < sizeInWords; i++) {
      buckets[i] = in.readLong();
    }*/
  }
  /**
   * clear the filter
   */
  public void clear() {
	// TODO Auto-generated method stub
	  CountingBloomFilter emptyOne=new CountingBloomFilter(vectorSize,nbHash,hashType);
	  //CountingBloomFilter cb = this.getCopy();
	  this.and(emptyOne);
	  emptyOne=null;
	  
  }

public int getSize() {
	// TODO Auto-generated method stub
	return this.vectorSize;
}

	/**
	 * set bit
	 * @param pos
	 */
	public void setBit(List<Integer> pos) {
	// TODO Auto-generated method stub
		Iterator<Integer> ier = pos.iterator();
		while(ier.hasNext()){
			setBit(ier.next());		
		}
}
	/**
	 * delete the bits
	 * @param pos
	 */
	public void deleteBit(List<Integer> pos) {
		// TODO Auto-generated method stub
		Iterator<Integer> ier = pos.iterator();
		while(ier.hasNext()){
			deleteBit(ier.next());
			
		}
	}

	public void setBit(Set<Integer> pos) {
		// TODO Auto-generated method stub
		Iterator<Integer> ier = pos.iterator();
		while(ier.hasNext()){
			setBit(ier.next());		
		}
	}
  
	public void deleteBit(Set<Integer> pos) {
		// TODO Auto-generated method stub
		Iterator<Integer> ier = pos.iterator();
		while(ier.hasNext()){
			deleteBit(ier.next());
			
		}
	}
	
	public static void main(String[] args){
		//test the counter bloom filter

		CountingBloomFilter test=new CountingBloomFilter(1024*2,8,Hash.MURMUR_HASH);
		
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

	
	public byte[] toBytes() {
		// TODO Auto-generated method stub
		byte[] out = new byte[this.values.length*ByteArrays.INT_SIZE_IN_BYTES];
		for(int i=0;i<this.values.length;i++){
			int val = this.values[i];
			byte[] valByte = ByteArrays.toByteArray(val);
			System.arraycopy(valByte, 0, out, (i)*ByteArrays.INT_SIZE_IN_BYTES, ByteArrays.INT_SIZE_IN_BYTES);
		}
		return out;
	}
	/**
	 * 
	 * @param rawBytes4Array
	 * @return
	 */
	public static CountingBloomFilter readBytes(byte[] rawBytes4Array,int offset,int numHash,int ArraySize,int hashType){
		
		CountingBloomFilter one = new CountingBloomFilter(ArraySize,numHash,hashType);
		//read by integer
		for(int i=0;i<ArraySize;i++){
			int startPos = i*ByteArrays.INT_SIZE_IN_BYTES;
			one.values[i]=ByteArrays.getInt(rawBytes4Array, offset+startPos, ByteArrays.INT_SIZE_IN_BYTES);
		}
		return one;
	}

	/**
	 * reset
	 */
	public void reset() {
		// TODO Auto-generated method stub
		this.clear();
	}
	
}