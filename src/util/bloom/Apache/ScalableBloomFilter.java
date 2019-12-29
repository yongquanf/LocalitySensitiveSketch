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
import java.util.Random;
import java.util.Vector;

import util.bloom.Apache.Hash.Hash;


/**
 * Implements a <i>scalable Bloom filter</i>, as defined in the INFOCOM 2006 paper.
 * <p>
 * A dynamic Bloom filter (DBF) makes use of a <code>s * m</code> bit matrix but
 * each of the <code>s</code> rows is a standard Bloom filter. The creation 
 * process of a DBF is iterative. At the start, the DBF is a <code>1 * m</code>
 * bit matrix, i.e., it is composed of a single standard Bloom filter.
 * It assumes that <code>n<sub>r</sub></code> elements are recorded in the 
 * initial bit vector, where <code>n<sub>r</sub> <= n</code> (<code>n</code> is
 * the cardinality of the set <code>A</code> to record in the filter).  
 * <p>
 * As the size of <code>A</code> grows during the execution of the application,
 * several keys must be inserted in the DBF.  When inserting a key into the DBF,
 * one must first get an active Bloom filter in the matrix.  A Bloom filter is
 * active when the number of recorded keys, <code>n<sub>r</sub></code>, is 
 * strictly less than the current cardinality of <code>A</code>, <code>n</code>.
 * If an active Bloom filter is found, the key is inserted and 
 * <code>n<sub>r</sub></code> is incremented by one. On the other hand, if there
 * is no active Bloom filter, a new one is created (i.e., a new row is added to
 * the matrix) according to the current size of <code>A</code> and the element
 * is added in this new Bloom filter and the <code>n<sub>r</sub></code> value of
 * this new Bloom filter is set to one.  A given key is said to belong to the
 * DBF if the <code>k</code> positions are set to one in one of the matrix rows.
 * <p>
 * Originally created by
 * <a href="http://www.one-lab.org">European Commission One-Lab Project 034819</a>.
 *
 * @see Filter The general behavior of a filter
 * @see BloomFilter A Bloom filter
 * 
 * @see <a href="http://www.cse.fau.edu/~jie/research/publications/Publication_files/infocom2006.pdf">Theory and Network Applications of Dynamic Bloom Filters</a>
 */
public class ScalableBloomFilter extends Filter {
  /**
	 * 
	 */
	private static final long serialVersionUID = 8785391817192113353L;

	/**
	 * size
	 */
	public int m0=128;
	/**
	 * space
	 */
	public int s=2;
	/**
	 * r
	 */
	public double filledFactorThreshold=0.5;
	/**
	 * false positive rate
	 */
	public double P=Math.pow(10, -6);
	/**
	 * P_0
	 */
	public double P0=-1;

  /**
   * The matrix of Bloom filter.
   */
  private BloomFilter[] matrix;

  /**
   * Zero-args constructor for the serialization.
   */
  public ScalableBloomFilter() { }

  /**
   * Constructor.
   * <p>
   * Builds an empty Dynamic Bloom filter.
   * @param vectorSize The number of bits in the vector.
   * @param nbHash The number of hash function to consider.
   * @param hashType type of the hashing function (see
   * {@link org.apache.hadoop.util.hash.Hash}).
   * @param nr The threshold for the maximum number of keys to record in a
   * dynamic Bloom filter row.
   */
  public ScalableBloomFilter(int vectorSize, int nbHash, int hashType, 
		  double _filledRatio, double _P,int _s) { 
    super(vectorSize, nbHash, hashType);

    /**
     * parameters
     */
    m0=vectorSize;
    s=_s;
    filledFactorThreshold=_filledRatio;
    P=_P;
    P0 = (1-filledFactorThreshold)*P;
    
    matrix = new BloomFilter[1];
    
    int k0=(int)Math.round(Math.log(1/P0)/Math.log(2));
    /**
     * first Bloom filter
     */
    matrix[0] = new BloomFilter(this.vectorSize, k0, this.hashType);
  }

  @Override
  public void add(Key key) {
    if (key == null) {
      throw new NullPointerException("Key can not be null");
    }

    BloomFilter bf = getActiveStandardBF();

    if (bf == null) {
      addRow();
      bf = matrix[matrix.length - 1];
      //currentNbRecord = 0;
    }

    bf.add(key);

    //currentNbRecord++;
  }

  /**
   * and operations with the other DBF
   */
  public ScalableBloomFilter andOperation(Filter filter) {
    if (filter == null
        || !(filter instanceof ScalableBloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      	return null;
      //throw new IllegalArgumentException("filters cannot be and-ed");
    }

    ScalableBloomFilter dbf = (ScalableBloomFilter)filter;

    /*if (dbf.matrix.length != this.matrix.length || dbf.nr != this.nr) {
      throw new IllegalArgumentException("filters cannot be and-ed");
    }

    for (int i = 0; i < matrix.length; i++) {
      matrix[i].and(dbf.matrix[i]);
    }*/
    
    BloomFilter[] bfs=new BloomFilter[this.matrix.length*dbf.matrix.length];
    int ind=0;
    for(int from=0;from<this.matrix.length;from++){
    	for(int to=0;to<dbf.matrix.length;to++){
    		//get the copy
    		bfs[ind]=this.matrix[from].getCopy();
    		//get the intersection
    		bfs[ind].and(dbf.matrix[to]);    		
    		ind++;
    	}
    }
    ScalableBloomFilter bf=new ScalableBloomFilter();
    bf.matrix=bfs;
    return bf;
  }

  @Override
  public boolean membershipTest(Key key) {
    if (key == null) {
      return true;
    }

    for (int i = 0; i < matrix.length; i++) {
      if (matrix[i].membershipTest(key)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public void not() {
    for (int i = 0; i < matrix.length; i++) {
      matrix[i].not();
    }
  }

  @Override
  public void or(Filter filter) {
    if (filter == null
        || !(filter instanceof ScalableBloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new IllegalArgumentException("filters cannot be or-ed");
    }

    ScalableBloomFilter dbf = (ScalableBloomFilter)filter;

    if (dbf.matrix.length != this.matrix.length ) {
      throw new IllegalArgumentException("filters cannot be or-ed");
    }
    for (int i = 0; i < matrix.length; i++) {
      matrix[i].or(dbf.matrix[i]);
    }
  }

  @Override
  public void xor(Filter filter) {
    if (filter == null
        || !(filter instanceof ScalableBloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new IllegalArgumentException("filters cannot be xor-ed");
    }
    ScalableBloomFilter dbf = (ScalableBloomFilter)filter;

    if (dbf.matrix.length != this.matrix.length ) {
      throw new IllegalArgumentException("filters cannot be xor-ed");
    }

    for(int i = 0; i<matrix.length; i++) {
        matrix[i].xor(dbf.matrix[i]);
    }
  }

  @Override
  public String toString() {
    StringBuilder res = new StringBuilder();

    for (int i = 0; i < matrix.length; i++) {
      res.append(matrix[i]);
      res.append(Character.LINE_SEPARATOR);
    }
    return res.toString();
  }

  // Writable

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(matrix.length);
    for (int i = 0; i < matrix.length; i++) {
      matrix[i].write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    int len = in.readInt();
    matrix = new BloomFilter[len];
    for (int i = 0; i < matrix.length; i++) {
      matrix[i] = new BloomFilter();
      matrix[i].readFields(in);
    }
  }

  /**
   * Adds a new row to <i>this</i> dynamic Bloom filter.
   */
  private void addRow() {
    BloomFilter[] tmp = new BloomFilter[matrix.length + 1];

    for (int i = 0; i < matrix.length; i++) {
      tmp[i] = matrix[i];
    }

    /**
     * the new Bloom filter
     */
    int mi=(int)(m0*Math.round(Math.pow(s,matrix.length-1)));
    int k0=matrix[0].nbHash;
    int ki = (int)Math.round(k0 + (matrix.length)*(Math.log(1/filledFactorThreshold)/Math.log(2) ));
    
    
    tmp[tmp.length-1] = new BloomFilter(mi, ki, hashType);

    matrix = tmp;
  }

  /**
   * Returns the active standard Bloom filter in <i>this</i> dynamic Bloom filter.
   * @return BloomFilter The active standard Bloom filter.
   * 			 <code>Null</code> otherwise.
   */
  private BloomFilter getActiveStandardBF() {
	/**
	 * false positive rate  
	 */
    if (matrix[matrix.length - 1].getAveragedObservedFalsePositive()>P0*Math.pow(filledFactorThreshold, matrix.length - 1)) {
      return null;
    }

    return matrix[matrix.length - 1];
  }
  
 public static void main(String[] args){
	  
	 
	 ScalableBloomFilter test=new ScalableBloomFilter();
	  
	  
	  Vector<Key> vec=new Vector<Key>(1);
	  Random r =new Random(1);	
	  
	  
	  int size=0;
	  int num=20;
	  
	  for(int j=0;j<num;j++){
		  
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
  * clear the dynamic filter
  */
   public void clear() {
	// TODO Auto-generated method stub
	   for(int i=0;i<this.matrix.length;i++){
		   this.matrix[i]=null;
	   }
	   this.matrix=null;
	   matrix = new BloomFilter[1];
	   matrix[0] = new BloomFilter(vectorSize, nbHash, hashType);



	   
	 /*  DynamicBloomFilter emptyOne=new DynamicBloomFilter(vectorSize,nbHash,hashType,nr);
	   emptyOne.matrix=new BloomFilter[this.matrix.length];
	   for(int i=0;i<this.matrix.length;i++){
		   emptyOne.matrix[i]=new BloomFilter(this.vectorSize, this.nbHash, this.hashType);
	   }
	   this.and(emptyOne);
	   
	   for(int i=0;i<this.matrix.length;i++){
		   emptyOne.matrix[i].clear();
		   emptyOne.matrix[i]=null;
	   }
	   emptyOne=null;*/	   
  }

   /**
    * get the size of the filter
    * @return
    */
   public int getSize() {
	// TODO Auto-generated method stub
	   int size=0;
	   for(int i=0;i<matrix.length;i++){
		   size+=matrix[i].vectorSize;
	   }
	   return size;
   }

   /**
    * get a copy of the Bloom
    * @return
    */
public ScalableBloomFilter getCopy() {
	// TODO Auto-generated method stub
	ScalableBloomFilter db=new ScalableBloomFilter();
	
	db.vectorSize=this.vectorSize;
	db.nbHash=this.nbHash;
	db.hashType=this.hashType;
	//copy the Bloomfilter matrix
	db.matrix=new BloomFilter[this.matrix.length];
	for(int i=0;i<this.matrix.length;i++){
		db.matrix[i]=new BloomFilter(vectorSize,nbHash,hashType);
		db.matrix[i].or(this.matrix[i]);
	}
	return db;
}

@Override
public void and(Filter filter) {
	// TODO Auto-generated method stub
	System.err.println("not implemented");
}
   
/**
 * filled factor of the bf
 * @param idx
 * @return
 */
	public double filledFactor(int idx){
		return matrix[idx].getAveragedObservedFalsePositive();
	}

	/**
	 * observed fp
	 * @return
	 */
	public double getAveragedObservedFalsePositive() {
	// TODO Auto-generated method stub
		double fp2=1;
		
		int n=matrix.length;
		
		for(int i=0;i<n;i++){
			fp2*=(1-matrix[i].getAveragedObservedFalsePositive());
		}
		
		return 1-fp2;
	}

	/**
	 * a prior false positive rate
	 * @return
	 */
	public double getAveragedPriorFP() {
		// TODO Auto-generated method stub
		  
		double fp2=1;
		
		int n=matrix.length;
		
		for(int i=0;i<n;i++){
			fp2*=(1-matrix[i].getAveragedPriorFP());
		}
		
		return 1-fp2;
	  }
	
	
}
