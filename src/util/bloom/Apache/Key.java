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
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import org.pcap4j.util.ByteArrays;

//import com.sun.corba.se.impl.oa.poa.ActiveObjectMap.Key;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.prp.POut;

import util.async.WritableComparable;



/**
 * The general behavior of a key that must be stored in a filter.
 * 
 * @see Filter The general behavior of a filter
 */
public class Key implements WritableComparable<Key> {
	
static Log log =new Log(Key.class);
	
  /** Byte value of key */
  byte[] bytes;
  
  /**
   * The weight associated to <i>this</i> key.
   * <p>
   * <b>Invariant</b>: if it is not specified, each instance of 
   * <code>Key</code> will have a default weight of 1.0
   */
  double weight;

  /** default constructor - use with readFields */
  public Key() {}

  /**
   * Constructor.
   * <p>
   * Builds a key with a default weight.
   * @param value The byte value of <i>this</i> key.
   */
  public Key(byte[] value) {
    this(value, 1.0);
  }

  
  
  /**
   * Constructor.
   * <p>
   * Builds a key with a specified weight.
   * @param value The value of <i>this</i> key.
   * @param weight The weight associated to <i>this</i> key.
   */
  public Key(byte[] value, double weight) {
    set(value, weight);
  }

  /**
   * ip address
   * @param address
   * @param address2
   * @param value
   */
  public Key(byte[] address, byte[] address2, byte[] address3) {
	// TODO Auto-generated constructor stub
	  byte[] a = ByteArrays.concatenate(address, address2);
	  byte[] b = ByteArrays.concatenate(a,address3);
	  set(b,0);
}

/**
   * @param value
   * @param weight
   */
  public void set(byte[] value, double weight) {
    if (value == null) {
      throw new IllegalArgumentException("value can not be null");
    }
    this.bytes = value;
    this.weight = weight;
  }
  
  /** @return byte[] The value of <i>this</i> key. */
  public byte[] getBytes() {
    return this.bytes;
  }

  /** @return Returns the weight associated to <i>this</i> key. */
  public double getWeight() {
    return weight;
  }

  /**
   * Increments the weight of <i>this</i> key with a specified value. 
   * @param weight The increment.
   */
  public void incrementWeight(double weight) {
    this.weight += weight;
  }

  /** Increments the weight of <i>this</i> key by one. */
  public void incrementWeight() {
    this.weight++;
  }

  @Override
  public boolean equals(Object o) {
    /*if (!(o instanceof Key)) {
      return false;
    }*/
	  if(o==null){return false;}
    return this.compareTo((Key)o) == 0;
  }
  
  @Override
  public int hashCode() {
    int result = 0;
    for (int i = 0; i < bytes.length; i++) {
      result ^= Byte.valueOf(bytes[i]).hashCode();
    }
    result ^= Double.valueOf(weight).hashCode();
    return result;
  }

  // Writable

  public void write(DataOutput out) throws IOException {
    out.writeInt(bytes.length);
    out.write(bytes);
    out.writeDouble(weight);
  }
  
  public void readFields(DataInput in) throws IOException {
    this.bytes = new byte[in.readInt()];
    in.readFully(this.bytes);
    weight = in.readDouble();
  }
  
  // Comparable
  
  public int compareTo(Key other) {
    int result = this.bytes.length - other.getBytes().length;
    for (int i = 0; result == 0 && i < bytes.length; i++) {
      result = this.bytes[i] - other.bytes[i];
      //System.out.println("byteDiff: "+result+", idx="+i);
    }
    
    if (result == 0) {
      result = Double.valueOf(this.weight - other.weight).intValue();
    }
    return result;
  }
  
  public Key makeCopy(){
	  return new  Key(this.bytes,this.weight); 
  }
  
  public String toString(){
	  return POut.toString(bytes);
  }
  
  public static void main(String[] args){
	  
	  
	  int a=10000;
	  byte[]  bs = ByteBuffer.allocate(4).putInt(a).array();
	  Key s =new Key(bs);
	  
	  System.out.println("s: "+s.toString());
	  
	  //Key s2 =s.makeCopy();
	  byte[]  bs2 = ByteBuffer.allocate(4).putInt(a).array();
	  Key s2=new Key(bs2);
	  
	  System.out.println(s.equals(s2));
	  
	  System.exit(-1);
	  
	  Set<Key> keyList=new HashSet<Key>(1);
	  
	  keyList.add(s.makeCopy());
	  
	  System.out.println(keyList.contains(s2));
	  
	  
	  while(true){
		  
			//break until enough keys
			if(keyList.size()==10){
				break;
			}
			
		  
		  byte[] key=new byte[4];
		  	  
		  //r.nextBytes( key );
		  key=ByteBuffer.allocate(4).putInt(100).array();
		  

  	  
			//r.nextBytes( key );
  
			Key key1=new Key(key);
			
			  log.info(key1.toString());
			
			  boolean test=keyList.contains(key1);
			  
			  
			  
			//same key, continue;
			if(test){
				log.warn("repeated key!!");
				continue;
			}else{
				keyList.add(key1);
			}
					 
		  }
	  
  }

public int length() {
	// TODO Auto-generated method stub
	return this.bytes.length;
}
}