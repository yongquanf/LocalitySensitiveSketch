/**
 * Ono Project
 *
 * File:         Util.java
 * RCS:          $Id: Util.java,v 1.11 2008/03/18 15:23:36 drc915 Exp $
 * Description:  Util class (see below)
 * Author:       David Choffnes
 *               Northwestern Systems Research Group
 *               Department of Computer Science
 *               Northwestern University
 * Created:      Aug 28, 2006 at 11:21:35 AM
 * Language:     Java
 * Package:      edu.northwestern.ono.util
 * Status:       Experimental (Do Not Distribute)
 *
 * (C) Copyright 2006, Northwestern University, all rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
 */
package util.async;



import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;

import edu.harvard.syrah.prp.Stat;

import util.bloom.Apache.Key;

/**
 * @author David Choffnes &lt;drchoffnes@cs.northwestern.edu&gt;
 *
 * The Util class provides utility functions for the package.
 */
public class Util<T> {
    
	static BufferedReader in;

	private static Util self;
    
    
    public static interface PingResponse {
    	public void response(double rtt);
    }

    
    public static long currentGMTTime(){
    	return System.currentTimeMillis()-TimeZone.getDefault().getRawOffset();
    } 
    
    public synchronized static Util getInstance(){

	    	if (self == null){
	    		self = new Util();
	    	}
    	
    	return self;
    }

    /**
     * choose a random object
     * @param myHashSet
     * @return
     */
    public static Object getRandomItem(Set set){
    	
    	int size = set.size();
    	int item = new Random().nextInt(size); // In real life, the Random object should be rather more shared than this
    	int i = 0;
    	for(Object obj : set)
    	{
    	    if (i == item)
    	        return obj;
    	    i = i + 1;
    	}
    	return null;
    }
    
    public static Object getRandomItem(List set){
    	if(set==null||set.isEmpty()){
    		return null;
    	}
    	int size = set.size();
    	int item = new Random().nextInt(size); // In real life, the Random object should be rather more shared than this
    	int i = 0;
    	return set.get(item);
    }
    
    public static int combinationNumber(int n){
    	return -1;
    	
    }
    
    
    public static int[] generateSequenceArray(int start,int separator,int end){
    	
    	List<Integer> ss=new ArrayList<Integer>();
    	int A=start;
    	while(A<=end){
    		ss.add(A);
    		
    		A=A+separator; 		
    	}
    	
    	int [] array=new int[ss.size()];
    	for(int i=0;i<array.length;i++){
    		array[i]=ss.get(i);
    	}
    	ss.clear();
    	ss=null;
    	return array;
    }
    
 public static double[] generateSequenceArray(double start,double separator,double end){
    	
    	List<Double> ss=new ArrayList<Double>();
    	double A=start;
    	while(A<=end){
    		ss.add(A);
    		
    		A=A+separator; 		
    	}
    	
    	double [] array=new double[ss.size()];
    	for(int i=0;i<array.length;i++){
    		array[i]=ss.get(i);
    	}
    	ss.clear();
    	ss=null;
    	return array;
    }

    /**
     * generate a set of random ids
     * @param num
     * @return
     */
   public static List<Integer> generateRandomIntegers(int num){
	   Set<Integer> set=new HashSet<Integer>(num);
	   
	   Random r=new Random(System.currentTimeMillis());
	   
	   while(set.size()<num){
		   set.add(r.nextInt());
	   }
	   //save to the list
	   List<Integer> list=new ArrayList<Integer>(num);
	   list.addAll(set);
	   //clear the set
	   set.clear();
	   set=null;
	   //return
	   return list;
   }

	/**
	 * create keys, not in totalIDs
	 * @param totalIDs
	 * @param n
	 * @return
	 */
	public static List<Integer> generateRandomIntegers(int curIntersect,List<Integer> totalIDs,
			int num) {
		
		 Set<Integer> avoided=new HashSet<Integer>(totalIDs);
		
		// TODO Auto-generated method stub
		  Set<Integer> set=new HashSet<Integer>(num);
		  
		  Random r=new Random(System.currentTimeMillis());
		//first half, not in  
		  int totalDiff=num-curIntersect;
		  if(totalDiff>0){
			  //add different keys
			  int tt;
			   while(set.size()<totalDiff){			   
				   tt=r.nextInt();
				   //add when not in
				   if(!avoided.contains(tt)){
					   set.add(tt);
				   }
				   
			   }  	  
		  }//different keys
		  avoided.clear();
		  avoided=null;
		   
		  int yourSize=totalIDs.size();
		  int id;
		 // randomly select a number of keys
		 while(set.size()<num){
			 id=totalIDs.get(r.nextInt(yourSize));
			 set.add(id);	 
		 }
		   
		   
		   //save to the list
		   List<Integer> list=new ArrayList<Integer>(num);
		   list.addAll(set);
		   //clear the set
		   set.clear();
		   set=null;
		   //return
		   
		   
		   return list;
	}
	
   
   public static List<Long> generateRandomLongs(int num){
	   Set<Long> set=new HashSet<Long>(num);
	   
	   Random r=new Random(System.currentTimeMillis());
	   
	   while(set.size()<num){
		   set.add(r.nextLong());
	   }
	   //save to the list
	   List<Long> list=new ArrayList<Long>(num);
	   list.addAll(set);
	   //clear the set
	   set.clear();
	   set=null;
	   //return
	   return list;
   }
   
   /**
    * get a subset
    * @param raw
    * @param num
    * @return
    */
   public static List<Integer> getSubSet(Set<Integer> raw,int num){
	   if(raw==null||raw.size()==0){
		   System.err.println("failed to choose subset, null");
		   return null;
	   }
	   //Set<Integer> set=new HashSet<Integer>(num);
	   int minSize=Math.min(raw.size(), num);
	   Random rnd=new Random(System.currentTimeMillis());
	   
	   List<Integer> rawList =new ArrayList(raw);
	   
	   Collections.shuffle(rawList, rnd);
	
	   return rawList;
   }
   
   public static boolean containValue(int[] hs,int val){
	   for(int i=0;i<hs.length;i++){
		   if(hs[i]==val){
			   return true;
		   }
	   }
	   return false;
   }
   
   public static int uniformDist=0;
   public static int zipfDist=1;
   
   public static boolean createTwoSetsWithFixedIntersections(int SizeDistribution,
			int fixedIntersection,List Original, List b	   
		   ){
	   if(Original==null||Original.isEmpty()||b==null){
		   return false;
	   }
	   int totalSize=Original.size();
	   int remainingSize=Original.size()-fixedIntersection;
	   if(remainingSize<0){
		  return false; 
	   }
	   
	   /**
	    * r
	    */
	   Random r=new Random(System.currentTimeMillis());
	   
	   b.clear();
	   
	   
	   if(remainingSize==0){
			  b.addAll(Original);
			  return true;
		   }
   
	   //now we configure B
	   	   
	  
	   Set common =new HashSet(fixedIntersection);
	   //common item
	   int idx;
	   int count=0;
	 while(count<fixedIntersection){
		idx=r.nextInt(totalSize);
		if(idx<0||idx>=totalSize){
			 continue;
		 }
		
		while(common.contains(Original.get(idx))){
			idx=r.nextInt(totalSize);
		}
		common.add(Original.get(idx));	
		count++;
	 }
	/* //find remaining Nodes
	 List remainingNodes=new ArrayList(remainingSize);
	 Iterator ier = Original.iterator();
	 while(ier.hasNext()){
		 Object tt = ier.next();
		 if(common.contains(tt)){
			continue; 
		 }else{
			 remainingNodes.add(tt);
		 }
	 }
	 Collections.shuffle(remainingNodes, r);
	 */
	 b.addAll(common);
	 //b.addAll(remainingNodes.subList(0, r.nextInt(remainingSize)));
	 
	 
	 //clear
	 //remainingNodes.clear();
	 //remainingNodes=null;
	 common.clear();
	 common=null;
	 
	 
	   
	  return true; 
  }
   
   public static boolean createTwoSetsWithFixedIntersections2(int SizeDistribution,
			int fixedIntersection,List Original, List a, List b	   
		   ){
	   if(Original==null||Original.isEmpty()||a==null||b==null){
		   return false;
	   }
	   int totalSize=Original.size();
	   int remainingSize=Original.size()-fixedIntersection;
	   if(remainingSize<0){
		  return false; 
	   }
	   
	   /**
	    * r
	    */
	   Random r=new Random(System.currentTimeMillis());
	   
	   a.clear();
	   b.clear();
	   
	   
	   if(remainingSize==0){
			  a.addAll(Original);
			  b.addAll(Original);
			  return true;
		   }
	   
	   a.addAll(Original);

	   
	   //now we configure B
	   	   
	  
	   Set common =new HashSet(fixedIntersection);
	   //common item
	   int idx;
	   int count=0;
	 while(count<fixedIntersection){
		idx=r.nextInt(totalSize);
		if(idx<0||idx>=totalSize){
			 continue;
		 }
		
		while(common.contains(Original.get(idx))){
			idx=r.nextInt(totalSize);
		}
		common.add(Original.get(idx));	
		count++;
	 }
	/* //find remaining Nodes
	 List remainingNodes=new ArrayList(remainingSize);
	 Iterator ier = Original.iterator();
	 while(ier.hasNext()){
		 Object tt = ier.next();
		 if(common.contains(tt)){
			continue; 
		 }else{
			 remainingNodes.add(tt);
		 }
	 }
	 Collections.shuffle(remainingNodes, r);
	 */
	 b.addAll(common);
	 //b.addAll(remainingNodes.subList(0, r.nextInt(remainingSize)));
	 
	 
	 //clear
	 //remainingNodes.clear();
	 //remainingNodes=null;
	 common.clear();
	 common=null;
	 
	 
	   
	  return true; 
   }
   /**
    * create the random list of items, based on the distribution scheme
    * @param SizeDistribution
    * @param totalNumber
    * @param fixedIntersection
    * @param a
    * @param b
    */
   public static boolean createTwoSetsWithFixedIntersections0(int SizeDistribution,
	int fixedIntersection,List Original, List a, List b	   
   ){
	   if(Original==null||Original.isEmpty()||a==null||b==null){
		   return false;
	   }
	   int totalSize=Original.size();
	   int remainingSize=Original.size()-fixedIntersection;
	   if(remainingSize<0){
		  return false; 
	   }
	   
	   /**
	    * r
	    */
	   Random r=new Random(System.currentTimeMillis());
	   
	   a.clear();
	   b.clear();
	   
	   if(remainingSize==0){
		  a.addAll(Original);
		  b.addAll(Original);
		  return true;
	   }
	   
	   if(SizeDistribution==uniformDist){
		 
		   int sizeA=fixedIntersection+r.nextInt(remainingSize);
		   int sizeB=fixedIntersection+r.nextInt(remainingSize);
			 
		   Set AA = new HashSet(sizeA);
		   Set BB = new HashSet(sizeB);
		   
		   Set common =new HashSet(fixedIntersection);
		   //common item
		   int idx;
		   int count=0;
		 while(count<fixedIntersection){
			idx=r.nextInt(totalSize);
			if(idx<0||idx>=totalSize){
				 continue;
			 }
			
			while(common.contains(Original.get(idx))){
				idx=r.nextInt(totalSize);
			}
			common.add(Original.get(idx));	
			count++;
		 }
		 
		 int repeated=50;
		 //add items for AA
		 while(AA.size()<sizeA){
			 if(repeated<=0){
				 break;
			 }
			 
			 idx=r.nextInt(totalSize);
			 if(idx<0||idx>=totalSize){
				 repeated--;
				 continue;
			 }
				while(repeated>0&&(AA.contains(Original.get(idx))||common.contains(Original.get(idx)))){
					idx=r.nextInt(totalSize);
					repeated--;
				}
				AA.add(Original.get(idx));
		 }
		 
		 
		 repeated=50;
		 while(BB.size()<sizeA){
			 
			 if(repeated<=0){
				 break;
			 }
			 
			 idx=r.nextInt(totalSize);
			 if(idx<0||idx>=totalSize){
				 repeated--;
				 continue;
			 }
				while(repeated>0&&(AA.contains(Original.get(idx))||BB.contains(Original.get(idx))||common.contains(Original.get(idx)))){
					idx=r.nextInt(totalSize);
					repeated--;
				}
				
			BB.add(Original.get(idx));
		 }
		 a.addAll(AA);
		 a.addAll(common);
		 b.addAll(common);
		 b.addAll(BB);
		 
		 common.clear();
		 common=null;
		 AA.clear();
		 BB.clear();
		 AA=null;
		 BB=null;
		 
		 return true;
		   
	   }
	   
	   if(SizeDistribution==zipfDist){
		   int n=totalSize;
		   int skew=4;
		   ZipfGenerator zipGen=new ZipfGenerator(n,skew);
		   
		   int sizeA=fixedIntersection+r.nextInt(remainingSize);
		   int sizeB=sizeA;
		   
		   createTwoSetsWithZipfDistribution(Original, sizeA, sizeB, a, b, zipGen);
		   
		   
		   return true;
	   }
	   
	   return false;
   }
   
   /**
    * construct the Zipf generator
    * @param SizeDistribution
    * @param Original
    * @param a
    * @param b
    * @param distGen
    * @return
    */
   public static boolean createTwoSetsWithZipfDistribution(
		   List Original,int sizeA,int sizeB, List a, List b,ZipfGenerator distGen	   	   
   ){
	   int totalSize=Original.size();
	   //int sizeA=distGen.next();
	  // int sizeB=distGen.next();
	   
	   //construct for user A
	   //Random r=new Random(System.currentTimeMillis());
	   
	   int idx=0;
		   
	   int count=0;
		 while(count<sizeA){
			idx=distGen.next();
			if(idx<0||idx>=totalSize){
				 continue;
			 }
			
			if(a.contains(Original.get(idx))){
				//idx=distGen.next();
				count++;
				continue;
				
			}
			a.add(Original.get(idx));	
			count++;
		 }
	   
	   //construct for user B
		 count=0;
		 while(count<sizeB){
			idx=distGen.next();
			if(idx<0||idx>=totalSize){
				 continue;
			 }
			
			while(b.contains(Original.get(idx))){
				//idx=distGen.next();
				count++;
				continue;
			}
			b.add(Original.get(idx));	
			count++;
		 }
	   
	   return true;
   }
   
   
   /**
    * equal test
    * @param a
    * @param b
    * @return
    */
   public static boolean equalsByte(byte[] a, byte[] b){
	   boolean isEqual=true;
	   for(int i=0;i<a.length;i++){
		   if(a[i]!=b[i]){
			   return false;
		   }
	   }
	   return true;
   }
   /**
    * xor the byte
    * @param a
    * @param b
    * @return
    */
   public static byte[] xor(byte[] a, byte[] b){
	   byte[] e = new byte[a.length];
	   for ( int i=0; i<a.length; i++ )
	      {
	      e[i] = (byte)( a[i] ^ b[i] );
	      }
	   return e;
   }
   /**
    * Integer
    * @param raw
    * @return
    */
   public static List<Key> getKeysInteger(Collection<Integer> raw){
	   if(raw==null||raw.size()==0){
		   System.err.println("failed to choose subset, null");
		   return null;
	   }
	   
	   //Random rnd=new Random(System.currentTimeMillis());
	  
	   
	   int minSize=raw.size();
	   

	   //save the result
	   List<Key> rawList =new ArrayList<Key>(raw.size());
	   
	   //iterator
	   Iterator<Integer> ier = raw.iterator();
	   //counter of items
	   int curCounter=0;
	   Integer tmp;
	   byte[] bs;
	   
	   //BigInteger curBI;
	   //add items until full
	   while((ier.hasNext())&&(curCounter< minSize)){
		  tmp = ier.next();
		  bs=ByteBuffer.allocate(4).putInt(tmp).array();
		  //curBI=new BigInteger(tmp+"");
		  //bs=curBI.toByteArray();
		  rawList.add(new Key(bs));  
		  curCounter++;
	   } 
	   //Collections.shuffle(rawList, rnd);
	
	   return rawList;
   }
   /**
    * get the keys from neighbors
    * @param raw
    * @return
    */
   public static List<Key> getKeys(Set<Long> raw){
	   if(raw==null||raw.size()==0){
		   System.err.println("failed to choose subset, null");
		   return null;
	   }
	   
	   //Random rnd=new Random(System.currentTimeMillis());
	  
	   
	   int minSize=raw.size();
	   

	   //save the result
	   List<Key> rawList =new ArrayList<Key>(raw.size());
	   
	   //iterator
	   Iterator<Long> ier = raw.iterator();
	   //counter of items
	   int curCounter=0;
	   Long tmp;
	   byte[] bs;
	   //add items until full
	   while((ier.hasNext())&&(curCounter< minSize)){
		  tmp = ier.next();
		  bs=ByteBuffer.allocate(8).putLong(tmp).array();
		  rawList.add(new Key(bs));  
		  curCounter++;
	   } 
	   //Collections.shuffle(rawList, rnd);
	
	   return rawList;
   }
   
   public static List<Key> getKeys(List<Long> raw){
	   if(raw==null||raw.size()==0){
		   System.err.println("failed to choose subset, null");
		   return null;
	   }
	   
	   //Random rnd=new Random(System.currentTimeMillis());
	  
	   
	   int minSize=raw.size();
	   

	   //save the result
	   List<Key> rawList =new ArrayList<Key>(raw.size());
	   
	   //iterator
	   Iterator<Long> ier = raw.iterator();
	   //counter of items
	   int curCounter=0;
	   Long tmp;
	   byte[] bs;
	   //add items until full
	   while((ier.hasNext())&&(curCounter< minSize)){
		  tmp = ier.next();
		  bs=ByteBuffer.allocate(8).putLong(tmp).array();
		  rawList.add(new Key(bs));  
		  curCounter++;
	   } 
	   //Collections.shuffle(rawList, rnd);
	
	   return rawList;
   }
   
   public static <T> List<T> getRandomSubList(List<T> input, int subsetSize)
   {
       Random r = new Random();
       int inputSize = input.size();
       for (int i = 0; i < subsetSize; i++)
       {
           int indexToSwap = i + r.nextInt(inputSize - i);
           T temp = input.get(i);
           input.set(i, input.get(indexToSwap));
           input.set(indexToSwap, temp);
       }
       return input.subList(0, subsetSize);
   }
   
   
   /**
    * get a subset
    * @param raw
    * @param num
    * @return
    */
   public static List<Key> getSubSet4Keys(List<Integer> raw,int num){
	   if(raw==null||raw.size()==0||raw.size()<num){
		   System.err.println("failed to choose subset, null"+", total: "+raw.size()+", need: "+num);
		   return null;
	   }
	   List<Integer> subList;
	   if(num<raw.size()){
		   subList = getRandomSubList(raw,num);
	   }else{
		   subList=raw;
	   }
	   //Random rnd=new Random(System.currentTimeMillis());
	   //Collections.shuffle(raw,rnd);
	   
	   
	   //int minSize=Math.min(raw.size(), num);
	   

	   //save the result
	   List<Key> rawList =new ArrayList<Key>(subList.size());
	   
	   //iterator
	   Iterator<Integer> ier = subList.iterator();
	   //counter of items
	   //int curCounter=0;
	   Integer tmp;
	   byte[] bs;
	   //add items until full
	   while((ier.hasNext())){
		  tmp = ier.next();
		  bs=ByteBuffer.allocate(4).putInt(tmp).array();
		  rawList.add(new Key(bs));  
		 // curCounter++;
	   } 
	   //Collections.shuffle(rawList, rnd);
	   
	   return rawList;
   }
   
   
   
    /**
     * @param newEdge
     * @return
     */
    public static String getClassCSubnet(String ipaddress) {
        // TODO Auto-generated method stub
        return ipaddress.substring(0, ipaddress.lastIndexOf("."));
    }

	public static byte[] convertLong(long l) {
		byte[] bArray = new byte[8];
        ByteBuffer bBuffer = ByteBuffer.wrap(bArray);
        bBuffer.order(ByteOrder.LITTLE_ENDIAN);
        LongBuffer lBuffer = bBuffer.asLongBuffer();
        lBuffer.put(0, l);
        return bArray;
	}
	
	public static  byte[] convertShort(short s) {
		byte[] bArray = new byte[2];
        ByteBuffer bBuffer = ByteBuffer.wrap(bArray);
        bBuffer.order(ByteOrder.LITTLE_ENDIAN);
        ShortBuffer sBuffer = bBuffer.asShortBuffer();
        sBuffer.put(0, s);
        return bArray;
	}
	
	public static  byte[] convertInt(int s) {
		byte[] bArray = new byte[4];
        ByteBuffer bBuffer = ByteBuffer.wrap(bArray);
        bBuffer.order(ByteOrder.LITTLE_ENDIAN);
        IntBuffer sBuffer = bBuffer.asIntBuffer();
        sBuffer.put(0, s);
        return bArray;
	}
	
	public static  byte[] convertFloat(float s) {
		byte[] bArray = new byte[4];
        ByteBuffer bBuffer = ByteBuffer.wrap(bArray);
        bBuffer.order(ByteOrder.LITTLE_ENDIAN);
        FloatBuffer sBuffer = bBuffer.asFloatBuffer();
        sBuffer.put(0, s);
        return bArray;
	}
	
	public static long byteToLong(byte data[]){
		ByteBuffer bBuffer = ByteBuffer.wrap(data);
		bBuffer.order(ByteOrder.LITTLE_ENDIAN);
		LongBuffer  lBuffer = bBuffer.asLongBuffer();
		return lBuffer.get();
	}

	public static byte[] convertStringToBytes(String key) {
		try {
			return key.getBytes("ISO-8859-1");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public static String convertByteToString(byte[] value){
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			baos.write(value);
		
			return baos.toString("ISO-8859-1");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}

	/**
	 * xor with a set of keys
	 * @param idSum
	 * @param dAMinusB
	 * @return
	 */
	public static byte[] xor(byte[] idSum, HashSet<Key> dAMinusB) {
		// TODO Auto-generated method stub
		if(dAMinusB==null||dAMinusB.isEmpty()){
			return idSum;
		}
		Iterator<Key> ier = dAMinusB.iterator();
		while(ier.hasNext()){
			idSum=Util.xor(idSum, ier.next().getBytes());
		}
		return idSum;
	}

	/**
	 * product of current list
	 * @param currentFilledFactor
	 * @return
	 */
	public static double product(List<Double> currentFilledFactor) {
		// TODO Auto-generated method stub
		double v=1;
		Iterator<Double> ier = currentFilledFactor.iterator();
		while(ier.hasNext()){
			v*=ier.next();
		}
		return v;
	}

	/**
	 * k!
	 * @param nbHashLev_1_Bloom
	 * @return
	 */
	public static double factorize(int nbHashLev_1_Bloom) {
		// TODO Auto-generated method stub
		double a=1.0d;
		int v=nbHashLev_1_Bloom;
		while(v>0){
			a=a*v;
			v--;
		}
		return a;
	}

	/**
	 * plot the cdf
	 * @param dat
	 */
	public static void plotCDF(String str,String header,List<Double> dat){
		double[]dd=new double[dat.size()];
		for(int i=0;i<dat.size();i++){
			dd[i]=dat.get(i);
		}
		Stat s=new Stat(dd);
		//100 points
		s.calculate();
		double[] cdfVal = s.getCDF();
		BufferedWriter bufferedWriter = null;
		try{
		
			bufferedWriter = new BufferedWriter(new FileWriter(str,true));
			
			bufferedWriter.append(header);
			bufferedWriter.newLine();
			bufferedWriter.flush();
			
			for(int i=0;i<cdfVal.length;i++){
				bufferedWriter.append(cdfVal[i]+" ");
			}
			bufferedWriter.newLine();
			bufferedWriter.flush();
			bufferedWriter.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		
		cdfVal=null;
		dd=null;
		s=null;	
	}

	public static long tick() {
		// TODO Auto-generated method stub
		return System.currentTimeMillis();
	}

	public static double toc(long t0) {
		// TODO Auto-generated method stub
		return (System.currentTimeMillis()-t0)/1000.0;
	}

	/**
	 * binomial distributions
	 * @param n
	 * @param p
	 * @return
	 */
	public static int binomialTrials(int n,double p,long seed){
		Random r=new Random(seed);
		int sum=0;
		for(int i=0;i<n;i++){
			if(r.nextDouble()<p){
				sum++;
			}
		}
		return sum;
	}

	/**
	 * convert
	 * @param bwLevels
	 * @return
	 */
	public static String toString(double[] bwLevels) {
		// TODO Auto-generated method stub
		
		if(bwLevels==null||bwLevels.length==0){
			return "";
		}else{
			StringBuffer sb=new StringBuffer();
			for(int i=0;i< bwLevels.length;i++){
				sb.append( bwLevels[i]+" ");
			}
			return sb.toString();
		}
	}
	public static String toString(int[] bwLevels) {
		// TODO Auto-generated method stub
		
		if(bwLevels==null||bwLevels.length==0){
			return "";
		}else{
			StringBuffer sb=new StringBuffer();
			for(int i=0;i< bwLevels.length;i++){
				sb.append( bwLevels[i]+" ");
			}
			return sb.toString();
		}
	}
	public static String toString(long[] bwLevels) {
		// TODO Auto-generated method stub
		
		if(bwLevels==null||bwLevels.length==0){
			return "";
		}else{
			StringBuffer sb=new StringBuffer();
			for(int i=0;i< bwLevels.length;i++){
				sb.append( bwLevels[i]+" ");
			}
			return sb.toString();
		}
	}
	/**
	 * remove nodes, preserve the intersect and the remaining items
	 * @param totalIDs
	 * @param nodeA_Neighbor
	 * @param intersect
	 * @param nb
	 * @return
	 */
	public static List<Key> getSubSet4Keys(List<Key> totalIDs,
			List<Key> nodeA_Neighbor, int intersect, int nb) {
		// TODO Auto-generated method stub
		
		int toDelete = nodeA_Neighbor.size()-intersect;
		
		Iterator<Key> ier = nodeA_Neighbor.iterator();
		while(ier.hasNext()&&toDelete>0){
			Key tmp = ier.next();
			totalIDs.remove(tmp);						
			toDelete--;
		}		
		return totalIDs;
	}
	
}
