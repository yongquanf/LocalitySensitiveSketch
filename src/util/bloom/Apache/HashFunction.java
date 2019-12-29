/**
 *
 * Copyright (c) 2005, European Commission project OneLab under contract 034819 
 * (http://www.one-lab.org)
 * 
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

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.prp.POut;

import sun.util.logging.resources.logging;
import util.bloom.Apache.Hash.Hash;



/**
 * Implements a hash object that returns a certain number of hashed values.
 * 
 * @see Key The general behavior of a key being stored in a filter
 * @see Filter The general behavior of a filter
 */
public final class HashFunction implements Serializable{
	
	static Log log =new Log(HashFunction.class);
  /**
	 * 
	 */
	private static final long serialVersionUID = -943724018304693116L;

/** The number of hashed values. */
  private int nbHash;

  /** The maximum highest returned value. */
  private int maxValue;

  /** Hashing algorithm to use. */
  private Hash hashFunction;
  
  /**
   * another hash
   */
  public static Hash SecondHash=Hash.getInstance(Hash.JENKINS_HASH);
  
  /**
   * the group of the hash function
   */
  private int indexGroup;
  
  /**
   * Constructor.
   * <p>
   * Builds a hash function that must obey to a given maximum number of returned values and a highest value.
   * @param maxValue The maximum highest returned value.
   * @param nbHash The number of resulting hashed values.
   * @param hashType type of the hashing function (see {@link Hash}).
   */
  public HashFunction(int maxValue, int nbHash, int hashType,int indexGroup) {
    if (maxValue <= 0) {
      throw new IllegalArgumentException("maxValue must be > 0");
    }
    
    if (nbHash <= 0) {
      throw new IllegalArgumentException("nbHash must be > 0");
    }

    /**
     * index of the group
     */
    this.indexGroup=indexGroup;
    
    this.maxValue = maxValue;
    this.nbHash = nbHash;
    this.hashFunction = Hash.getInstance(hashType);
    if (this.hashFunction == null)
      throw new IllegalArgumentException("hashType must be known");
  }

  /**
   * Ericfu, reset the maxValue
   * @param newMaxValue
   */
   public void resetMaxValue(int newMaxValue){
	   this.maxValue = newMaxValue;
   }
   
  
  /**
   * default
   * @param maxValue
   * @param nbHash
   * @param hashType
   */
  public HashFunction(int maxValue, int nbHash, int hashType) {
	    if (maxValue <= 0) {
	      throw new IllegalArgumentException("maxValue must be > 0");
	    }
	    
	    if (nbHash <= 0) {
	      throw new IllegalArgumentException("nbHash must be > 0");
	    }

	    /**
	     * index of the group
	     */
	    this.indexGroup=1;
	    
	    this.maxValue = maxValue;
	    this.nbHash = nbHash;
	    this.hashFunction = Hash.getInstance(hashType);
	    if (this.hashFunction == null)
	      throw new IllegalArgumentException("hashType must be known");
	  }
  
  
  /** Clears <i>this</i> hash function. A NOOP */
  public void clear() {
  }

  /**
   * Hashes a specified key into several integers.
   * @param k The specified key.
   * @return The array of hashed values.
   */
  public int[] hash(Key k){
	  
      byte[] b = k.getBytes();
      if (b == null) {
        throw new NullPointerException("buffer reference is null");
      }
      if (b.length == 0) {
        throw new IllegalArgumentException("key length must be > 0");
      }
      int[] result = new int[nbHash];
      //System.out.print("\n ===============\n");
      
      //the index of the group
      int intialGroup_ = hashFunction.hash(b,indexGroup+1);
      
      for (int i = 0, initval = 0; i < nbHash; i++) {
    	  
      //calculate the hash function	     	  
	  initval = hashFunction.hash(b, (initval+intialGroup_)%Integer.MAX_VALUE);
	  
	  result[i] = Math.abs(initval % maxValue);
	 // System.out.print(", "+result[i]);
      }
    //  System.out.print("\n===============\n");
      return result;
  }
  
  public int[] hashFake(Key k,List<Integer> Z){
	  
      byte[] b = k.getBytes();
      if (b == null) {
        throw new NullPointerException("buffer reference is null");
      }
      if (b.length == 0) {
        throw new IllegalArgumentException("key length must be > 0");
      }
      int[] result = new int[nbHash];
      //System.out.print("\n ===============\n");
      
      //the index of the group
      int intialGroup_ = hashFunction.hash(b,indexGroup+1);
      
      for (int i = 0, initval = 0; i < nbHash; i++) {
    	  
      //calculate the hash function	     	  
	  initval = hashFunction.hash(b, (initval+intialGroup_)%Integer.MAX_VALUE);
	  
	  result[i] = Math.abs(initval % maxValue);
	 // System.out.print(", "+result[i]);
      }
    //  System.out.print("\n===============\n");
      return result;
  }
  
  /**
   * use the address based hashing, 
   * @param k
   * @param Z
   * @return
   */
  public int[] hash11(Key k,List<Integer> Z){
      byte[] b = k.getBytes();
      if (b == null) {
        throw new NullPointerException("buffer reference is null");
      }
      if (b.length == 0) {
        throw new IllegalArgumentException("key length must be > 0");
      }
      int[] result = new int[nbHash];
      
      int val;
      int seed=0;
      
      for (int i = 0; i < nbHash; i++) {
    	  
    	  //seed=i;    	  
    	  for(int j=0;j<Z.size();j++){
    		  seed=hashFunction.hash(ByteBuffer.allocate(4).putInt(seed).array(),
    				  R3+Z.get(j)*R4);
    	  }
    	  
    	  val= hashFunction.hash(b,seed)+i*
    	  SecondHash.hash(b, seed);
    	  
    	  result[i] = Math.abs(val % maxValue);
      }
      
      
      return result;
  }
  
  /**
   * cache the seed
   * @param k
   * @param seed
   * @return
   */
  public int[] hash(Key k,int seed){
      byte[] b = k.getBytes();
      if (b == null) {
        throw new NullPointerException("buffer reference is null");
      }
      if (b.length == 0) {
        throw new IllegalArgumentException("key length must be > 0");
      }
      int[] result = new int[nbHash];
      
      int val;
      
      /*
      int seed=0;
      
      //log.info("Z: "+POut.toString(Z));
      
	  for(int j=0;j<Z.size();j++){
		  seed=SecondHash.hash(ByteBuffer.allocate(4).putInt(R1+R2*Z.get(j)).array(),
				  seed);
	  }
	  */
	  //seed=Math.abs(seed;
	  
	  //log.info("Hash: "+seed);
	  //seed=10000;
      
      
      
      
      
    //============================================  
    //basic  
/*      int vv=seed;
      int index=1;
      int ii;
      
      int firstHash,secondHash;
      
      for (int i = 0; i < nbHash; i++) {
    	  index= i; 	  
    	  ii=(int)Math.round(Math.pow(index,2));
    	  
    	  firstHash=hashFunction.hash(b,seed);
    	  
    	  secondHash=hashFunction.hash(b,ii);
    	  
    	  vv=firstHash+ii*secondHash;
    	  result[i] = Math.abs(vv % maxValue);
      }*/
      
     
    for (int i = 0; i < nbHash; i++) {
    	  
    	  seed= hashFunction.hash(b,seed);
    	  
    	  result[i] = Math.abs(seed % maxValue);
      }
     
      
    //============================================ 
      
      
     /* digestFunction.reset();
      
      //init
      digestFunction.update(ByteBuffer.allocate(4).putInt(seed).array());
      
      int kk = 0;
      byte salt=0;
      
      while (kk < nbHash) {
          byte[] digest;
          synchronized (digestFunction) {
        	  
              digestFunction.update(salt);
               salt++;
             // salt=hashFunction.hash(digestFunctionSeed.digest(ByteBuffer.allocate(4).putInt((R1+R2*salt)).array()));
              
              digest = digestFunction.digest(b);                
          }
      
          for (int i = 0; i < digest.length/4 && kk < nbHash; i++) {
              int h = 0;
              for (int j = (i*4); j < (i*4)+4; j++) {
                  h <<= 8;
                  h |= ((int) digest[j]) & 0xFF;
              }
              result[kk] =  Math.abs(h % maxValue);
              kk++;
          }
      }  */
      
      return result;
  }
  
  
  
  
  
  public int[] hashOldButGood(Key k,List<Integer> Z){
      byte[] b = k.getBytes();
      if (b == null) {
        throw new NullPointerException("buffer reference is null");
      }
      if (b.length == 0) {
        throw new IllegalArgumentException("key length must be > 0");
      }
      int[] result = new int[nbHash];
      
      int val;
      int seed=0;
      
      //log.info("Z: "+POut.toString(Z));
      
	  for(int j=0;j<Z.size();j++){
		  seed=SecondHash.hash(ByteBuffer.allocate(4).putInt(R1+R2*Z.get(j)).array(),
				  seed);
	  }
	  
	  //seed=Math.abs(seed;
	  
	  //log.info("Hash: "+seed);
	  //seed=10000;
      for (int i = 0; i < nbHash; i++) {
    	  
    	  seed= hashFunction.hash(b,R3+R4*seed);
    	  
    	  result[i] = Math.abs(seed % maxValue);
      }
      return result;
  }
  
  /**
   * hash
   * @param key
   * @param Z
   * @return
   */
  	public int[] hash1130(Key key,List<Integer> Z){
	  
  		byte[] b = key.getBytes();
        if (b == null) {
          throw new NullPointerException("buffer reference is null");
        }
        if (b.length == 0) {
          throw new IllegalArgumentException("key length must be > 0");
        }
        int[] result = new int[nbHash];
        
        //log.info("Z: "+POut.toString(Z));
        //reset the digest
        //digestFunction.reset();
       
        //init
        digestFunction.reset();  
        digestFunctionSeed.reset();
        int salt = 0;
        
  	  for(int j=0;j<Z.size();j++){
  		synchronized (digestFunctionSeed) {
  			digestFunctionSeed.update(ByteBuffer.allocate(4).putInt((R1+R2*Z.get(j))%Integer.MAX_VALUE).array());
  		}
  	  }
  	  
      int k = 0;
    
      while (k < nbHash) {
          byte[] digest;
          synchronized (digestFunction) {
              digestFunction.update(ByteBuffer.allocate(4).putInt((R3+R4*salt)).array());
               //salt++;
              salt=hashFunction.hash(digestFunctionSeed.digest(ByteBuffer.allocate(4).putInt((R1+R2*salt)).array()));
              
              digest = digestFunction.digest(b);                
          }
      
          for (int i = 0; i < digest.length/4 && k < nbHash; i++) {
              int h = 0;
              for (int j = (i*4); j < (i*4)+4; j++) {
                  h <<= 8;
                  h |= ((int) digest[j]) & 0xFF;
              }
              result[k] =  Math.abs(h % maxValue);
              k++;
          }
      }              

	  return result;
  }
  
  /**
   * Generates digests based on the contents of an array of bytes and splits the result into 4-byte int's and store them in an array. The
   * digest function is called until the required number of int's are produced. For each call to digest a salt
   * is prepended to the data. The salt is increased by 1 for each call.
   *
   * @param data specifies input data.
   * @param hashes number of hashes/int's to produce.
   * @return array of int-sized hashes
   */
  public static int[] createHashes(byte[] data, int hashes) {
      int[] result = new int[hashes];

      int k = 0;
      byte salt = 0;
      while (k < hashes) {
          byte[] digest;
          synchronized (digestFunction) {
              digestFunction.update(salt);
              salt++;
              digest = digestFunction.digest(data);                
          }
      
          for (int i = 0; i < digest.length/4 && k < hashes; i++) {
              int h = 0;
              for (int j = (i*4); j < (i*4)+4; j++) {
                  h <<= 8;
                  h |= ((int) digest[j]) & 0xFF;
              }
              result[k] = h;
              k++;
          }
      }
      return result;
  }
  
  static final String hashName = "MD5"; // MD5 gives good enough accuracy in most circumstances. Change to SHA1 if it's needed
  static final MessageDigest digestFunction;
  static { // The digest method is reused between instances
      MessageDigest tmp;
      try {
          tmp = java.security.MessageDigest.getInstance(hashName);
      } catch (NoSuchAlgorithmException e) {
          tmp = null;
      }
      digestFunction = tmp;
  }
  
  static final MessageDigest digestFunctionSeed;
  static { // The digest method is reused between instances
      MessageDigest tmp;
      try {
          tmp = java.security.MessageDigest.getInstance(hashName);
      } catch (NoSuchAlgorithmException e) {
          tmp = null;
      }
      digestFunctionSeed = tmp;
  }
  
  /**
   * hash using the seed
   * @param k
   * @param Z
   * @return
   */
  public int[] hash11171(Key k,List<Integer> Z){
      byte[] b = k.getBytes();
      if (b == null) {
        throw new NullPointerException("buffer reference is null");
      }
      if (b.length == 0) {
        throw new IllegalArgumentException("key length must be > 0");
      }
      int[] result = new int[nbHash];
      
      int val;
      int seed=0;
      
      for (int i = 0; i < nbHash; i++) {
    	  
    	  seed=R1+i*R2;
    	  //seed=i;
    	  
    	  for(int j=0;j<Z.size();j++){
    		  seed=hashFunction.hash(ByteBuffer.allocate(4).putInt(seed).array(),
    				  R3+Z.get(j)*R4);
    	  }
    	  
    	  val= hashFunction.hash(b,seed);
    	  
    	  result[i] = Math.abs(val % maxValue);
      }
      
      
    /*  for (int i = 0; i < nbHash; i++) {
    	  
    	  seed=R1+i*R2;
    	  //seed=i;
    	  
    	  for(int j=0;j<Z.size();j++){
    		  seed=hashFunction.hash(ByteBuffer.allocate(4).putInt(R3+Z.get(j)*R4).array(),
    				  seed);
    	  }
    	  
    	  val= hashFunction.hash(b,seed);
    	  
    	  result[i] = Math.abs(val % maxValue);
      }*/
      
      
      /*
       * 
       * Trial 1
       * int keyPrefix = hashFunction.hash(b,0);          

      for(int j=1;j<Z.size();j++){
    	  keyPrefix=hashFunction.hash(ByteBuffer.allocate(4).putInt(keyPrefix).array(),
    			  Z.get(j));
      }
      int initval =0;           
      for (int i = 0; i < nbHash; i++) {
    	  
    	  initval = hashFunction.hash(ByteBuffer.allocate(4).putInt(keyPrefix).array(),
    			  initval);
	  
    	  //System.out.println(initval+" "+initval % Integer.MAX_VALUE);
	  
    	  result[i] = Math.abs(initval % maxValue);
      }
      */
      return result;
  }
  
 /**
  * init value 
  */
	static int R1=0x5ed50e23;
	static int R2=0x1b75e0d1;
	
	static int R3=0x1c2372da;
	static int R4=0x4da17b1f;
	
	/**
	 * hash function
	 * @param s
	 * @param Z
	 * @return
	 */
	public int[] hash00(Key s,List<Integer> Z){
		
		int[] result = new int[nbHash];
		int seed=0;
		
		int key = ByteBuffer.wrap(s.bytes).getInt();
				
		int v1=CRC32.getInstance().getValue(Integer.toString(0),key);

		System.out.println("key: "+key+" value1: "+v1);
		
		for(int j=0;j<nbHash;j++){
		
		
		seed=CRC32.getInstance().getValue(Integer.toString(j),
				((R1+Z.get(0)*R2)));
		
		for(int i=1;i<Z.size();i++){
			seed=CRC32.getInstance().getValue(Integer.toString(seed),
					(R1+Z.get(i)*R2));
		}
		int t=(CRC32.getInstance().getValue(Integer.toString(v1),
				seed)) ;
		result[j]=Math.abs(t% maxValue);
		System.out.println("seed: "+seed+" hash: "+result[j]+" before: "+t);
		}
		return result;
	}
	/**
	 * CRC hash function
	 * @param s
	 * @param index
	 * @param BFLen
	 * @return
	 */
	public int CRC_Hash(Key s,int index,int BFLen){
		
		int keyVal = ByteBuffer.wrap(s.bytes).getInt();
		
		int v1=CRC32.getInstance().getValue(Integer.toString(0),keyVal);
		
		int v2=CRC32.getInstance().getValue(Integer.toString(v1), 
				R1+index*R2);
		
		v1=Math.abs(v2 % BFLen);
		return v1;
		
	}
/**
 * calculate the hash using the location based naming as the seeds  
 * @param k
 * @param Z
 * @return
 */
public int[] hash0(Key k,List<Integer> Z){
	  
		//System.out.println("prefix length: "+Z.length());
	
      byte[] b = k.getBytes();
      if (b == null) {
        throw new NullPointerException("buffer reference is null");
      }
      if (b.length == 0) {
        throw new IllegalArgumentException("key length must be > 0");
      }
      int[] result = new int[nbHash];
      //System.out.print("\n ===============\n");
      
      //the index of the group
      //int intialGroup_ = hashFunction.hash(b,indexGroup+1);
      
      for (int i = 0, initval = 0; i < nbHash; i++) {
    
    	  
    	  initval= hashFunction.hash(
    			  ByteBuffer.allocate(4).putInt((R1+(i)*R2)%Integer.MAX_VALUE).array()		  
    			  ,(R1+Z.get(0)*R2)%Integer.MAX_VALUE);	
    	  for(int idx=1;idx<Z.size();idx++){
    		  initval= hashFunction.hash(
        			  ByteBuffer.allocate(4).putInt(initval).array()		  
        			  ,(R1+Z.get(idx)*R2)%Integer.MAX_VALUE);
    	  }
    	  //hash value
    	  initval =  hashFunction.hash(
    		b, (R1+initval*R2)%Integer.MAX_VALUE);
      
	  result[i] = Math.abs(initval % maxValue);
	 // System.out.print(", "+result[i]);
      }
    //  System.out.print("\n===============\n");
      return result;
  }
}