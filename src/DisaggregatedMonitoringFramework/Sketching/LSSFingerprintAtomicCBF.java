package DisaggregatedMonitoringFramework.Sketching;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pulsar.shade.org.codehaus.jackson.map.ser.std.StdArraySerializers.ByteArraySerializer;
import org.apache.pulsar.shade.org.codehaus.jackson.util.ByteArrayBuilder;
import org.eclipse.jetty.util.log.Log;
import org.pcap4j.util.ByteArrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.Maps;
import com.google.common.primitives.Bytes;

import DisaggregatedMonitoringFramework.Ingest.Pub4PCapStreamKVTable;
import DisaggregatedMonitoringFramework.control.KVTable;
import DisaggregatedMonitoringFramework.control.LogicController;
import ECS.ClusterStatic;
import ECS.ClusterStatic.Pair;
import edu.harvard.syrah.prp.POut;
import sun.util.logging.resources.logging;
import util.bloom.Apache.BloomFilter;
import util.bloom.Apache.BloomFilterFactory;
import util.bloom.Apache.CountingBloomFilter;
import util.bloom.Apache.Filter;
import util.bloom.Apache.Key;
import util.bloom.Apache.Hash.Hash;
import util.bloom.Apache.Hash.LongHashFunction;

public class LSSFingerprintAtomicCBF {

	private static final Logger log = LoggerFactory.getLogger(LSSFingerprintAtomicCBF.class);
	
	
	/**
	 * lock for heavy change test
	 */
	ReentrantLock lock=null;
	
	private final byte fingerprintLastByteMask = (byte)0xff;
	
	//to record fingerprint
	//	protected ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
	
	/**
	 * group membership tester
	 */
	public AtomicReferenceArray<CountingBloomFilter> clusterGrouper;
	/**
	 * expected fp
	 */
	 double expectedFP = 0.001;
	
	/**
	 * record sketch
	 */
	public AtomicReferenceArray<AtomicReferenceArray<LSSEntryFinger>> LSSTable;
	
	//for overflow
	Map<Key,Integer> shadowMapGlobal = null;
	
	public Map<Key, Integer> getShadowMapGlobal() {
		return shadowMapGlobal;
	}


	/**
	 * cluster center
	 */
	public AtomicReferenceArray<Float> clusterCenters;
	public AtomicReferenceArray<Float>  clusterdensity;
	
	/**
	 * parameters
	 */
	 int clusterCount;
	 int bucketCount;
	 int expectedNumItems;
	
	 int fingerprintLen;
	
	//hash function
	public AtomicReferenceArray<LongHashFunction> LongHashFunction4PosHash;
		
	//equal or density
	//public boolean useEqual = true;
	
	
	/**
	 * constructor
	 * @param bufferedWriter 
	 * @param _n, umber of flow entries
	 * @param _c, cluster number
	 * @param _b, bucket count,
	 * @param _fp, false positive of bloom filters
	 * @param traces, flow trace
	 */
	public LSSFingerprintAtomicCBF(int _n, int _c, int _b,float _fp,Collection<Integer> traces,
			int FingerLen, BufferedWriter bufferedWriter){
		expectedNumItems = _n;
		clusterCount = _c;
		bucketCount = _b;
		expectedFP = _fp;
		
		fingerprintLen = FingerLen;
		
		//for shadown
		shadowMapGlobal = Maps.newConcurrentMap();
		
		lock = new ReentrantLock();
		
		//cluster grouper
		 clusterGrouper = new AtomicReferenceArray<CountingBloomFilter>(clusterCount);
		 for(int i=0;i<clusterCount;i++){
			 int[] conf = BloomFilter.ArraySize4ItemAndProb(expectedNumItems, expectedFP);
			 
			 ////System.out.println("BF: "+POut.toString(conf));
			 log.info("expected: "+expectedNumItems+", "+ expectedFP+", "+POut.toString(conf));
			 clusterGrouper.set(i,new CountingBloomFilter(conf[0],conf[1],Hash.MURMUR_HASH));
		 }
		 
		 
		
		//hash
		LongHashFunction4PosHash = new AtomicReferenceArray<LongHashFunction>(clusterCount);
		for(int i=0;i<clusterCount;i++){
			LongHashFunction4PosHash.set(i, LongHashFunction.xx(i));
		}
		
		clusterCenters = new AtomicReferenceArray<Float>(clusterCount);
		for(int i=0;i<_c;i++){
			clusterCenters.set(i, 0.0f);
		}
		
		clusterdensity=new AtomicReferenceArray<Float>(clusterCount);
		for(int i=0;i<_c;i++){
			clusterdensity.set(i, 0.0f);
		}
				
		/**
		 * init cluster center, cluster centroid
		 */
		initClusterCenters(traces,bufferedWriter);
		
		 
		LSSTable = new AtomicReferenceArray<AtomicReferenceArray<LSSEntryFinger>>(clusterCount);
		for(int i=0;i<clusterCount;i++){
			//scaled by cluster size
			int arraySize = Math.round(bucketCount/clusterCount);
			//new array		
			AtomicReferenceArray<LSSEntryFinger> b = new AtomicReferenceArray<LSSEntryFinger>(arraySize);
			for(int j=0;j<arraySize;j++){
				b.set(j, new LSSEntryFinger());
			}
			//add
			LSSTable.set(i, b);
		}//equal size
		 
		
	}

	/**
	 * transform
	 * @param code, [counter, sum]
	 * @return
	 */
	public static int[] ReverseCounterAndSum(byte[] code){
		int[] out=new int[]{Pub4PCapStreamKVTable.getIntegerValue4KVBytes(code, 0),Pub4PCapStreamKVTable.getIntegerValue4KVBytes(code, ByteArrays.INT_SIZE_IN_BYTES)};
		return out;
	}

	/**
	 * 
	 * @param item
	 * @return
	 */
	public boolean matchShadow(byte[] item){
		return shadowMapGlobal.containsKey(new Key(item));
	}
	
	/**
	 * add to shadow
	 * @param item
	 */
	public int add2shadow(Key key,int val){
		int out = 0;
		
		
			 //synchronized access
			 boolean isLockAcquired;
			try {
				isLockAcquired = lock.tryLock(100, TimeUnit.MILLISECONDS);
				
				if(isLockAcquired ){
					 
					 try{ 
			
			if(!shadowMapGlobal.containsKey(key)){
				shadowMapGlobal.put(key, val);
				out = val;
			}else{
				//increment
				int val0 = shadowMapGlobal.get(key);
				out = val0 + val;
				shadowMapGlobal.replace(key,out );
				
			}
				 
			 }finally{
				 lock.unlock();
			 }
				 }
						
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return out;
			
	}
	
	
	private static byte[] longtoBytes(long data) {
		 return new byte[]{
		 (byte) ((data >> 56) & 0xff),
		 (byte) ((data >> 48) & 0xff),
		 (byte) ((data >> 40) & 0xff),
		 (byte) ((data >> 32) & 0xff),
		 (byte) ((data >> 24) & 0xff),
		 (byte) ((data >> 16) & 0xff),
		 (byte) ((data >> 8) & 0xff),
		 (byte) ((data >> 0) & 0xff),
		 };
		}

	/**
     * create a new fingerprint, with counter =val;
     * Note: 32-bit upper bound;
     * @param item
     * @return
     */
    public byte[] GetItemInfoByte(byte[] item) {
    	//ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    	
    	//Key key = new Key(item);
    	//key.hashCode()
    	
        
        //create fingerprint
        long hashVal = hashPos(item,0);
        
        //reset buffer
        //ByteBuffer buffer = ByteBuffer.allocate(item.length);
        //buffer.clear();
        //buffer.putLong(hashVal);
        //hash infor
        byte[] hash = longtoBytes(hashVal); //// sha1.digest(item);
        
        // Fingerprint
        byte[] fingerprint = new byte[fingerprintSizeInBytes()];
        
        for(int i=0; i < fingerprint.length; i++)
            fingerprint[i] = hash[(i+4)%hash.length];
        fingerprint[fingerprint.length-1] &= fingerprintLastByteMask;
        if(ByteUtil.isZero(fingerprint)) // Avoiding fingerprints with all zeros (they would be confused with 'no fingerprint' in the table)
            fingerprint[0] = 1;
        
        return fingerprint;
    }
    
	
	public static int fingerprintSizeInBytes() {
        return (int)Math.ceil(LogicController.FingerLen/8.0D);
    }
	
	public LSSFingerprintAtomicCBF(){
		
		//if(shadowMapGlobal==null){
	   shadowMapGlobal=Maps.newConcurrentMap();
		//}
		lock = new ReentrantLock();
	}

	
	/**
	 * cluster parametrs
	 */
	public final int maxIterations4Cluster = 100;
	 
	
	/**
	 * use trace to train cluster centers and cluster sizes
	 * @param traces
	 * @param bufferedWriter 
	 */
	public void initClusterCenters(Collection<Integer> traces, BufferedWriter bufferedWriter){
		/**
		 * cluster
		 */
		ClusterStatic  test  = new ClusterStatic(clusterCount,maxIterations4Cluster);
		 
		// Iterator<Double> iter = traces.iterator();
//		 while(iter.hasNext()){
//			//System.out.println(POut.toString(iter.next())+"\t");
//		}
		
		 Pair[] centers = test.KPPCluster(traces,bufferedWriter);
		
		
		try {
			bufferedWriter.write("cluster center: ");
			for(int i=0;i<centers.length;i++){
				clusterCenters.set(i, (float) centers[i].value);
				clusterdensity.set(i,(float) (centers[i].index+0.0f));
				bufferedWriter.write("(" +i+", "+clusterCenters.get(i)+", "+clusterdensity.get(i)+"), ");
			}
			bufferedWriter.write("\n");
			bufferedWriter.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		 
		centers = null;

	}
	/**
	 * 
	 * @param flowValue
	 * @return
	 */
	public int groupInputKV(int flowValue){
		int index=-1;
		float vat = Float.MAX_VALUE;
		for(int i=0;i<clusterCount;i++){
			float v= Math.abs(clusterCenters.get(i)- flowValue);
			if(v<vat){
				vat = v;
				index = i;
			}
		}
		return index;
	}
	
	public int groupInputKV(double flowValue){
		int index=-1;
		float vat = Float.MAX_VALUE;
		for(int i=0;i<clusterCount;i++){
			float v= (float) Math.abs(clusterCenters.get(i)- flowValue);
			if(v<vat){
				vat = v;
				index = i;
			}
		}
		return index;
	}
	
	/**
	 * group by cluster
	 * @param key
	 * @return
	 */
	public int findGroup4InputKey(Key key){
		int index = -1;
		//Key k = new Key(key.getBytes());
		for(int i=0;i<clusterGrouper.length();i++){
			if(clusterGrouper.get(i).membershipTest(key)){
				if(differentiateAmbiguousPos(i,key)){
					return i;
				}else{
					continue;
				}
			}
		}
		return index;
	}
	/**
	 * 
	 * @param i
	 * @param key
	 * @return
	 */
	private boolean differentiateAmbiguousPos(int arrayIndex, Key key) {
		// TODO Auto-generated method stub
		boolean isInShadow= shadowMapGlobal.containsKey(key);
		return isInShadow;		
	}

	/**
	 * list pos
	 * @param key
	 * @return
	 */
	public ArrayList<Integer> findGroup4InputKeyAll(Key key){
		ArrayList<Integer>  index  = new ArrayList<Integer>();
		//Key k = new Key(key.getBytes());
		for(int i=0;i<clusterGrouper.length();i++){
			if(clusterGrouper.get(i).membershipTest(key)){
				index.add(i);
			}
		}
		return index;
	}
	
	
	/**
	 * helper function
	 * @param id
	 * @param index
	 * @return
	 */
	public int hashPos(String id,int index){
		long longHash=LongHashFunction4PosHash.get(index).hashChars(id);//,N_HASHCHECK);
		return (int) Math.abs(longHash%Integer.MAX_VALUE);
	}
	
	public  int hashPos(Key id,int index){
		long longHash=LongHashFunction4PosHash.get(index).hashBytes(id.getBytes());//,N_HASHCHECK);
		return (int) Math.abs(longHash%Integer.MAX_VALUE);
	}
	
	public int hashPos(byte[] id,int index){
		long longHash=LongHashFunction4PosHash.get(index).hashBytes(id);//,N_HASHCHECK);
		return (int) Math.abs(longHash%Integer.MAX_VALUE);
	}
	
	public int hashPos(long id,int index){
		long longHash=LongHashFunction4PosHash.get(index).hashLong(id);//,N_HASHCHECK);
		return (int) Math.abs(longHash%Integer.MAX_VALUE);
	}
	
	
	/**
	 * key
	 * @param key
	 * @param val
	 * @return
	 */
	public boolean insert(Key key,int val){
		return insert(key.getBytes(),val);
	}
	
	/**
	 * double input
	 * @param key
	 * @param val
	 * @return
	 */
	public boolean insert(byte[] keyRawNotUsed,int RawVal){
		
		//null
		if(keyRawNotUsed==null || RawVal<=0){
			return false;
		}
		
		 //use fingerprint
		 Key keyRaw = null;
		 byte[] incomingKV0=null;
		 if(fingerprintSizeInBytes()!=keyRawNotUsed.length){
			 incomingKV0 = GetItemInfoByte(keyRawNotUsed);	 
			 keyRaw = new Key(incomingKV0);
		 }else{
			 incomingKV0 = keyRawNotUsed;
			 keyRaw = new Key(keyRawNotUsed);
		 }
		 
		 
		// keep the fingerprint, get the val
		int newValResult= add2shadow(keyRaw, RawVal);
		
		 if(newValResult<=0){
			 return false;
		 }
		 
		//query first
		int clusterIndex =  findGroup4InputKey(keyRaw);
		boolean notInBF = false;
		//insert next
		if(clusterIndex<0){	
			clusterIndex = groupInputKV(RawVal);
			notInBF = true;
		}
		if(clusterIndex<0){return false;}
		
		//array
		 AtomicReferenceArray<LSSEntryFinger> array = LSSTable.get(clusterIndex);
					 
		//find position
		int pos = hashPos(keyRaw,clusterIndex)%(array.length());
		
		//System.out.println("insert: "+clusterIndex+", "+array.length+", val: "+val+", to: "+pos);
		
		//assume unique id;
		LSSEntryFinger xx = array.get(pos);
		
		//new item
		if(newValResult==RawVal){
			//directly update the entry						
			xx.addEntry(RawVal,true);
			//add to bf
			if(notInBF){
				clusterGrouper.get(clusterIndex).add(keyRaw);
			}
		}else{
		//test the new cluster
			int newCluster=groupInputKV(newValResult);
			if(newCluster==clusterIndex){
				//do not change, update value, do not update bf
				xx.addEntry(RawVal, false);
			}else{
				//change, delete from current, move to the new cluster
				
				try{						
						if(matchShadow(keyRaw.getBytes())){
							xx.deleteEntry(newValResult-RawVal);
						//old cluster
							try{
								clusterGrouper.get(clusterIndex).delete(keyRaw);
							}catch(Exception e){
									e.printStackTrace();
									log.warn("false negative in BF! "+keyRaw+", "+newValResult+", "+RawVal);								
							}
						}
						//add to the new cluster
						//array
						array = LSSTable.get(newCluster);							 
						//find position
						pos = hashPos(keyRaw,newCluster)%(array.length());				
						//System.out.println("insert: "+clusterIndex+", "+array.length+", val: "+val+", to: "+pos);
						
						//assume unique id;
						xx = array.get(pos);
						xx.addEntry(newValResult,true);
						//update key
						clusterGrouper.get(newCluster).add(keyRaw);
						
				}catch(Exception e){
					e.printStackTrace();
					//searchClusterIndex();
				}
				
			}
			
		}
		
		return true;
		
	}
	
	
	public float query(Key keyRawNotUsed){
		
		//null
				if(keyRawNotUsed==null){
					return -1;
				}
				
				 //use fingerprint
				 Key keyRaw = null;
				 byte[] incomingKV0=null;
				 if(fingerprintSizeInBytes()!=keyRawNotUsed.length()){
					 incomingKV0 = GetItemInfoByte(keyRawNotUsed.getBytes());	 
					 keyRaw = new Key(incomingKV0);
				 }else{					 
					 keyRaw = keyRawNotUsed;
				 }
	
				
		//int clusterIndex =  findGroup4InputKey(keyRaw);
		ArrayList<Integer> clusterAll = findGroup4InputKeyAll(keyRaw);
		
		//not in array
		if(clusterAll.isEmpty()){
		if(shadowMapGlobal!=null&&!shadowMapGlobal.isEmpty()&&shadowMapGlobal.containsKey(keyRaw)){
			return shadowMapGlobal.get(keyRaw);
		}else{
			
			return -1;
		}
		}
		float sum = 0;
		//int count = 0;
		float trueVal = shadowMapGlobal.get(keyRaw);
		float currentVal=-1;
		
		Iterator<Integer> ier = clusterAll.iterator();
		while(ier.hasNext()){
			int clusterIndex = ier.next();
		//array
		AtomicReferenceArray<LSSEntryFinger> array = LSSTable.get(clusterIndex);
		//find position
		int pos = hashPos(keyRaw,clusterIndex)%(array.length());
		float avgVal = array.get(pos).getAvgEstimator();
		//update
		if(Math.abs(avgVal-trueVal)<(Math.abs(currentVal-trueVal))){
			currentVal = avgVal;
			sum+=avgVal;
		}
		}
		//average
		if(clusterAll.size()>1){
			log.info("ambiguous: "+keyRaw.toString()+" "+clusterAll.size()+" "+trueVal+" "+currentVal+" "+(sum/(clusterAll.size())));
		}
		
		clusterAll.clear();
		
		return sum;
	}
	
	/**
	 * sum of each query result
	 * @param keyRawNotUsed
	 * @return
	 */
	public float queryV2(Key keyRawNotUsed){
		
		//null
				if(keyRawNotUsed==null){
					return -1;
				}
				
				 //use fingerprint
				 Key keyRaw = null;
				 byte[] incomingKV0=null;
				 if(fingerprintSizeInBytes()!=keyRawNotUsed.length()){
					 incomingKV0 = GetItemInfoByte(keyRawNotUsed.getBytes());	 
					 keyRaw = new Key(incomingKV0);
				 }else{					 
					 keyRaw = keyRawNotUsed;
				 }
	
				
		//int clusterIndex =  findGroup4InputKey(keyRaw);
		ArrayList<Integer> clusterAll = findGroup4InputKeyAll(keyRaw);
		
		//not in array
		if(clusterAll.isEmpty()){
		if(shadowMapGlobal!=null&&!shadowMapGlobal.isEmpty()&&shadowMapGlobal.containsKey(keyRaw)){
			return shadowMapGlobal.get(keyRaw);
		}else{
			
			return -1;
		}
		}
		float sum = 0;
		int count = 0;
		Iterator<Integer> ier = clusterAll.iterator();
		while(ier.hasNext()){
			int clusterIndex = ier.next();
		//array
		AtomicReferenceArray<LSSEntryFinger> array = LSSTable.get(clusterIndex);
		//find position
		int pos = hashPos(keyRaw,clusterIndex)%(array.length());
		float avgVal = array.get(pos).getAvgEstimator();
		if(avgVal>0){
			sum+=avgVal;
			count++;
		}
		}
		//average
		sum/=(count);
		
		clusterAll.clear();
		
		return sum;
	}

	
	/**
	 * quey all matched
	 * @param key
	 * @return
	 */
	public float queryV0(Key keyRawNotUsed){
		
		//null
				if(keyRawNotUsed==null){
					return -1;
				}
				
				 //use fingerprint
				 Key keyRaw = null;
				 byte[] incomingKV0=null;
				 if(fingerprintSizeInBytes()!=keyRawNotUsed.length()){
					 incomingKV0 = GetItemInfoByte(keyRawNotUsed.getBytes());	 
					 keyRaw = new Key(incomingKV0);
				 }else{					 
					 keyRaw = keyRawNotUsed;
				 }
	
				
		int clusterIndex =  findGroup4InputKey(keyRaw);
		//not in array
		if(clusterIndex<0){
		if(shadowMapGlobal!=null&&!shadowMapGlobal.isEmpty()&&shadowMapGlobal.containsKey(keyRaw)){
			return shadowMapGlobal.get(keyRaw);
		}else{
			
			return -1;
		}
		}
		//array
		AtomicReferenceArray<LSSEntryFinger> array = LSSTable.get(clusterIndex);
		//find position
		int pos = hashPos(keyRaw,clusterIndex)%(array.length());
		float avgVal = array.get(pos).getAvgEstimator();
		return avgVal;
	}
	
	
	public ArrayList<Float> queryAllMatched(Key key){
	  ArrayList<Integer> clusterIndexes = findGroup4InputKeyAll(key);
	  if(clusterIndexes.isEmpty()){
		  return null;
	  }
	  
	  ArrayList<Float> out = new ArrayList<Float>();
	  //list all
	   for(int clusterIndex: clusterIndexes){
		//array
		   AtomicReferenceArray<LSSEntryFinger> array = LSSTable.get(clusterIndex);
		//find position
		int pos = hashPos(key,clusterIndex)%(array.length());
		float avgVal = array.get(pos).getAvgEstimator();
		 out.add(avgVal);
	   }
	   return out;
	}
	
	/**
	 * estimate the size
	 * @param flowSet
	 * @return
	 */
	public boolean CollectEstimatedSize(Map<Key,Double> flowSet){
		
		
		Iterator<Entry<Key, Double>> ier = flowSet.entrySet().iterator();
		while(ier.hasNext()){
			Entry<Key, Double> tmp = ier.next();
			Key key = tmp.getKey();
			double val = query(key);
			tmp.setValue(val);
		}
		
		
		return true;
	}
	
	/**
	 * number of distinct flows
	 * @return
	 */
	public int distinctFlows() {
		// TODO Auto-generated method stub
		/*int sum = 0;
		
		LSSEntryFinger item;
		int val;
	     int len = LSSTable.length()-1;
		while(len>0){
			AtomicReferenceArray<LSSEntryFinger> tmp = LSSTable.get(len);
			for(int i=0;i<tmp.length();i++){
				item = tmp.get(i);
				val = item.counter;
				if(val>0){
					sum += val;
				}
			}
			len--;
		}
		*/
		//sum+=this.shadowMapGlobal.size();
		if(shadowMapGlobal==null||shadowMapGlobal.isEmpty()){
			return 0;
		}else{
			return this.shadowMapGlobal.size();
		}
	}
	public void clear() {
		// TODO Auto-generated method stub
		this.clusterCenters = null;
		this.LSSTable = null;
		this.clusterdensity = null;
	}

	/**
	 * insert
	 * @param map
	 */
	public void insert(Map<Key, Integer> map) {
		// TODO Auto-generated method stub
		for(Entry<Key, Integer> entry: map.entrySet()){
			this.insert(entry.getKey(), entry.getValue());
		}		
	}
	/**
	 * query all
	 * @param items
	 * @return
	 */
	public Map<Key, Double> get(Set<Key> items){
		Map<Key, Double> map = new HashMap<Key, Double>();
		Iterator<Key> ier = items.iterator();
		while(ier.hasNext()){
			Key t = ier.next();
			double val = this.query(t);
			if(val<=0){
				continue;
			}else{
				map.put(t, val);
			}
		}
		return map;
	}

	/**
	 * builder
	 * @param _n
	 * @param _c
	 * @param _b
	 * @param _fp
	 * @param traces
	 * @param _useEqual
	 * @param numEntries
	 * @param FingerLen
	 * @param bufferedWriter
	 * @return
	 */
	public static LSSFingerprintAtomicCBF build(int _n, int _c, int _b,float _fp,Collection<Integer> traces,
			  int FingerLen, BufferedWriter bufferedWriter) {
		// TODO Auto-generated method stub
		return new LSSFingerprintAtomicCBF(_n, _c, _b, _fp,traces,  FingerLen,  bufferedWriter);
	}
	
	/**
	 * 
	 */
	public LSSFingerprintAtomicCBF clone(){
		
		LSSFingerprintAtomicCBF now = new LSSFingerprintAtomicCBF();
		
		now.expectedNumItems = this.expectedNumItems;
		now.clusterCount = this.clusterCount;
		now.bucketCount = this.bucketCount;
		now.expectedFP = this.expectedFP;
		
		now.fingerprintLen = this.fingerprintLen;
		
		//cluster grouper
		 now.clusterGrouper = new AtomicReferenceArray<CountingBloomFilter>(this.clusterCount);
		 for(int i=0;i<this.clusterCount;i++){
			 //int[] conf = BloomFilter.ArraySize4ItemAndProb(this.expectedNumItems, this.expectedFP);
			 
			 ////System.out.println("BF: "+POut.toString(conf));
			 CountingBloomFilter xx = this.clusterGrouper.get(i);
			 log.info("BFsize: "+xx.getSize());
			 now.clusterGrouper.set(i,xx.getCopy());
		 }
		 
		
		//hash
		now.LongHashFunction4PosHash = new AtomicReferenceArray<LongHashFunction>(this.clusterCount);
		for(int i=0;i<this.clusterCount;i++){
			now.LongHashFunction4PosHash.set(i, LongHashFunction.xx(i));
		}
		
		now.clusterCenters = new AtomicReferenceArray<Float>(this.clusterCount);
		for(int i=0;i<this.clusterCount;i++){
			now.clusterCenters.set(i, this.clusterCenters.get(i));
		}
		
		now.clusterdensity=new AtomicReferenceArray<Float>(this.clusterCount);
		for(int i=0;i<this.clusterCount;i++){
			now.clusterdensity.set(i,this.clusterdensity.get(i));
		}
				 
		 
		now.LSSTable = new AtomicReferenceArray<AtomicReferenceArray<LSSEntryFinger>>(this.clusterCount);
		for(int i=0;i<this.clusterCount;i++){
			//scaled by cluster size
			int arraySize = Math.round(this.bucketCount/this.clusterCount);
			//new array		
			AtomicReferenceArray<LSSEntryFinger> b = new AtomicReferenceArray<LSSEntryFinger>(arraySize);
			for(int j=0;j<arraySize;j++){
				b.set(j, this.LSSTable.get(i).get(j).copy());
			}
			//add
			now.LSSTable.set(i, b);
		}//equal size
		
		now.shadowMapGlobal.clear();
		
		return now;
		
	}
	
	/**
	 * assume fingerprint
	 * @param map
	 * @return
	 */
	public static byte[] Map2Bytes(Map<Key,Integer> map){
		if(map!=null&&!map.isEmpty()){
			//header three fields, integer,, num of items;key len, val len;
			//key1-keyn; val1-valn
			//int headerCount = 3;
			
			
			Set<Key> keyIndex = map.keySet();
			int numItems = keyIndex.size();
			
			int keyByteLen = fingerprintSizeInBytes();
			int valByteLen = ByteArrays.INT_SIZE_IN_BYTES;
			
			int entryByteLen =  keyByteLen + valByteLen;
			
						
			byte[] header=ByteArrays.concatenate(ByteArrays.concatenate(ByteArrays.toByteArray(numItems),
					ByteArrays.toByteArray(keyByteLen)),ByteArrays.toByteArray(valByteLen));
			
			byte[] payloads = new byte[header.length+(entryByteLen*numItems)];
			
			System.arraycopy(header, 0, payloads, 0, header.length);
			int offset = header.length;
			
			int index=0;
			Iterator<Key> ier = keyIndex.iterator();
			while(ier.hasNext()&&(index<numItems)){
				Key tmp = ier.next();
				if(map.containsKey(tmp)){
				Integer val = map.get(tmp);
				byte[] key = tmp.getBytes();
				//one item
				byte[] oneEntry = ByteArrays.concatenate(key,ByteArrays.toByteArray(
						val
						));
				
				System.arraycopy(oneEntry, 0, payloads, offset+index*oneEntry.length, oneEntry.length);
				index++;
				}
			}
			
			return payloads;
			
		}else{
			return null;
		}
	}
	/**
	 * header: three integers, num of items; key byte count; val byte count;
	 * @param mapByteArray
	 * @return
	 */
	public static Map<Key,Integer> Byte2Map( byte[] mapByteArray){
		if(mapByteArray==null||mapByteArray.length==0){
			return null;
		}
		
		int headerCount = 3;
		int offset = headerCount*ByteArrays.INT_SIZE_IN_BYTES;
		if(mapByteArray.length-offset<=0){
			return null;
		}
		
		
		//byte[] header = ByteArrays.getSubArray(mapByteArray,0,offset);
		int numItems = ByteArrays.getInt(mapByteArray, 0, ByteArrays.INT_SIZE_IN_BYTES);
		int keyByteLen = ByteArrays.getInt(mapByteArray, ByteArrays.INT_SIZE_IN_BYTES,ByteArrays.INT_SIZE_IN_BYTES);
		int valByteLen =ByteArrays.getInt(mapByteArray, 2*ByteArrays.INT_SIZE_IN_BYTES,ByteArrays.INT_SIZE_IN_BYTES);
		
		byte[] payload = ByteArrays.getSubArray(mapByteArray, offset,mapByteArray.length-offset);
		int oneEntryLen = keyByteLen+valByteLen;
		
		Map<Key,Integer> map=  Maps.newConcurrentMap();
		for(int i=0;i<numItems;i++){
			int startPos = i*oneEntryLen;
			byte[] item = ByteArrays.getSubArray(payload, startPos, oneEntryLen);
			//item
			Key key = new Key(ByteArrays.getSubArray(item, 0, keyByteLen));
			int val = ByteArrays.getInt(item, keyByteLen);
			//all zero, pass
			if(val<=0){
				continue;
			}
			
			map.put(key, val);
		}
		return map;
	}
	
	/**
	 * float to byte arrays,
	 * @return
	 */
	public static byte[] Float2Bytes(float[] clus){
		
		byte[] bytes = new byte[clus.length*ByteArrays.INT_SIZE_IN_BYTES];
		//iterate 0
		for(int i=0;i<clus.length;i++){		
			int val =Float.floatToIntBits(clus[i]);
			byte[] tmp =ByteArrays.toByteArray(val);
			System.arraycopy(tmp, 0, bytes, (i)*ByteArrays.INT_SIZE_IN_BYTES, ByteArrays.INT_SIZE_IN_BYTES);
		}
		return bytes;
	}
	/**
	 * reverse the cluster centers
	 * retrieve 4 bytes each time, get integer, transform to float
	 * @param rawbytes
	 * @return
	 */
	public static Float[] reverseFloatFromBytes(byte[] rawbytes){
		int numFloat = rawbytes.length/ByteArrays.INT_SIZE_IN_BYTES;
		byte[] bytes = new byte[ByteArrays.INT_SIZE_IN_BYTES];
		Float[] val = new Float[numFloat];
		
		for(int i=0;i<numFloat;i++){
			System.arraycopy(rawbytes, (i)*ByteArrays.INT_SIZE_IN_BYTES, bytes, 0, ByteArrays.INT_SIZE_IN_BYTES);
			int intVal = ByteArrays.getInt(bytes, 0);
			float floatVal = Float.intBitsToFloat(intVal);
			val[i] = floatVal;
		}
		return val;
	}
	
	/**
	 * 
	 * @return
	 */
	public byte[] LSSTransform2Bytes(){
		//published, nothing
		if(this.shadowMapGlobal.isEmpty()){
			return null;
		}
		//synchronized access shadow map
		byte[] shMap=null;		
		 boolean isLockAcquired;
			try {
				isLockAcquired = lock.tryLock(100, TimeUnit.MILLISECONDS);
				
				if(isLockAcquired ){
					 
					 try{ 
		shMap = Map2Bytes(this.shadowMapGlobal);
					 }finally{
						 lock.unlock();
					 }
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		
		if(shMap==null||shMap.length<=1){
			return null;
		}
		//cluster centers
		byte[] ccBytes = Float2Bytes(this.clusterCenters);
		//bf
		byte[] cbfBytes = BF2Bytes(this.clusterGrouper);
		//lss table
		byte[] lssBytes = LSS2Bytes(this.LSSTable);
		//shadow map
		
		//four fields
		//1.cluster center byte array size
		//2.cbf byte matrix size
		//3.lss byte matrix size
		//4. shadow map size
		int clusterCenterByteLen=ccBytes.length;
		int cbfLength = cbfBytes.length;
		int lssLen = lssBytes.length;
		int shadownLen = shMap.length;
		
		byte[] header = ByteArrays.concatenate(ByteArrays.concatenate(
				ByteArrays.concatenate(ByteArrays.toByteArray(clusterCenterByteLen), 
				ByteArrays.toByteArray(cbfLength)),ByteArrays.toByteArray(lssLen)),ByteArrays.toByteArray(shadownLen));
		
		return ByteArrays.concatenate(ByteArrays.concatenate(ByteArrays.concatenate(ByteArrays.concatenate(header,ccBytes),
				cbfBytes),lssBytes),shMap);
		
	}
	
	/**
	 * LSS bytes to ds
	 * @param rawLSS
	 */
	public static LSSFingerprintAtomicCBF Bytes2LSS(byte[] rawLSS){
		
		int headerNum = 4;
		int[] header= new int[headerNum];//0: clustercenterByteLen, 1: cbf+cbf-header len; 2: lss len;3, map byte len
		//byte[] tempInt = new byte[ByteArrays.INT_SIZE_IN_BYTES];
		//header
		for(int i=0;i<headerNum;i++){
			//System.arraycopy(rawLSS, i*ByteArrays.INT_SIZE_IN_BYTES, tempInt, 0, ByteArrays.INT_SIZE_IN_BYTES);
			header[i] = ByteArrays.getInt(rawLSS, i*ByteArrays.INT_SIZE_IN_BYTES,ByteArrays.INT_SIZE_IN_BYTES);
		}
		
		//offset header
		int offsetHeader = ByteArrays.INT_SIZE_IN_BYTES*headerNum;
		//number of clusters
		int numCluster = header[0]/ByteArrays.INT_SIZE_IN_BYTES;
		//cluster center
		byte[] tempClusCenterBytes=new byte[header[0]];
		System.arraycopy(rawLSS, offsetHeader, tempClusCenterBytes, 0, header[0]);
		Float[] clusterCenters = reverseFloatFromBytes(tempClusCenterBytes);
		//clear
		tempClusCenterBytes = null;
		
		//cbf 
		byte[] tempcbfBytes = new byte[header[1]];
		int offsetCBFHead = ByteArrays.INT_SIZE_IN_BYTES;		
		System.arraycopy(rawLSS,offsetHeader+header[0],tempcbfBytes,0,header[1]);
		int numHash = ByteArrays.getInt(tempcbfBytes, 0,offsetCBFHead);		
		AtomicReferenceArray<CountingBloomFilter> cbfs = reverseCBF(tempcbfBytes,numCluster,offsetCBFHead,numHash);
		tempcbfBytes = null;
		
		//lss
		byte[] tempLSS = new byte[header[2]];
		System.arraycopy(rawLSS,offsetHeader+header[0]+header[1],tempLSS,0,header[2]);		 
		//use configuration file
		int numBucket = LogicController.bucketCount/LogicController.clusterCount;
		AtomicReferenceArray<AtomicReferenceArray<LSSEntryFinger>> lsstable = reverseLSSTableBytes(tempLSS,
				numCluster,numBucket);
		tempLSS = null;
		
		//last shadow map
		byte[] tempShadowMap = new byte[header[3]];
		System.arraycopy(rawLSS, offsetHeader+header[0]+header[1]+header[2],tempShadowMap, 0, header[3]);
		//recover
		Map<Key, Integer> shadowMap = Byte2Map(tempShadowMap);
		
		
		//reconstruct the lss table
		LSSFingerprintAtomicCBF now = new LSSFingerprintAtomicCBF();
		
		now.expectedNumItems = (int) LogicController.NumberFlowsPerPeriod;
		now.clusterCount = LogicController.clusterCount;
		now.bucketCount = LogicController.bucketCount;
		now.expectedFP = LogicController.expectedFP;	
		now.fingerprintLen = LogicController.FingerLen;
		//set
		now.clusterCenters = new AtomicReferenceArray<Float>(clusterCenters);
		now.clusterGrouper = cbfs;
		now.LSSTable = lsstable;		
		now.shadowMapGlobal = (shadowMap==null)?(Maps.newConcurrentMap()):shadowMap;
		//hash
		//hash
		now.LongHashFunction4PosHash = new AtomicReferenceArray<LongHashFunction>(now.clusterCount);
		for(int i=0;i<now.clusterCount;i++){
			now.LongHashFunction4PosHash.set(i, LongHashFunction.xx(i));
		}
		
		return now;
	}
	
	


	/**
	 * reverse cbf
	 * @param tempcbfBytes
	 * @param numCBF
	 * @param ByteOffsetCBFHead
	 * @param _numHash
	 * @return
	 */
	private static AtomicReferenceArray<CountingBloomFilter> reverseCBF(byte[] tempcbfBytes, int numCBF, int ByteOffsetCBFHead, int _numHash) {
		// TODO Auto-generated method stub
		int offset =  ByteOffsetCBFHead;
		int numHash = _numHash;
		int ArraySize = (tempcbfBytes.length-ByteOffsetCBFHead)/(ByteArrays.INT_SIZE_IN_BYTES*numCBF);
		int oneByteLen = ArraySize*ByteArrays.INT_SIZE_IN_BYTES;
		int hashType = Hash.MURMUR_HASH;
		 AtomicReferenceArray<CountingBloomFilter> cbfAll = new AtomicReferenceArray<CountingBloomFilter>(numCBF);
		 
		for(int i=0;i<numCBF;i++){
			int offsetCBF = i*ArraySize*ByteArrays.INT_SIZE_IN_BYTES;
			byte[] rawBytes4Array = ByteArrays.getSubArray(tempcbfBytes, ByteOffsetCBFHead+offsetCBF, oneByteLen);
			cbfAll.set(i, CountingBloomFilter.readBytes(rawBytes4Array, 0, numHash, ArraySize, hashType));
		}
		
		return cbfAll;
	}



	/**
	 * LSS to bytes
	 * @param lssTable2
	 * @return
	 */
	private byte[] LSS2Bytes(AtomicReferenceArray<AtomicReferenceArray<LSSEntryFinger>> lssTable2) {
		// TODO Auto-generated method stub
		int numLSSBucketArrays = lssTable2.length();
		int numBucketsPerArray = lssTable2.get(0).length();
		int bytesPerbucket = lssTable2.get(0).get(0).byteNum();
		byte[] rawBytes = new byte[numLSSBucketArrays*numBucketsPerArray*bytesPerbucket];
		//traverse each array
		for(int i=0;i<numLSSBucketArrays;i++){
			int offsetArray = i*numBucketsPerArray*bytesPerbucket;
			//traverse each bucket
			for(int j=0;j<numBucketsPerArray;j++){
				byte[] byteBucket = lssTable2.get(i).get(j).toByteArray();
				int offsetBucet = j*bytesPerbucket;
				System.arraycopy(byteBucket, 0, rawBytes, offsetArray+offsetBucet, bytesPerbucket);
			}
			
		}
		return rawBytes;
	}

	/**
	 * reverse lss
	 * @param tempLSS
	 */
	private static AtomicReferenceArray<AtomicReferenceArray<LSSEntryFinger>> reverseLSSTableBytes(byte[] tempLSS,int numArray,int numBucket) {
		// TODO Auto-generated method stub
		AtomicReferenceArray<AtomicReferenceArray<LSSEntryFinger>> lsstable = new 
				AtomicReferenceArray<AtomicReferenceArray<LSSEntryFinger>>(numArray);
		
		//per bucket size
		int bucketByteSize = ByteArrays.INT_SIZE_IN_BYTES*2;
		int ArrayByteSize = numBucket*bucketByteSize;
		byte[] temp;
		//num array
		for(int i=0;i<numArray;i++){
			//offset of the current table
			int offsetArray= i*ArrayByteSize;
			AtomicReferenceArray<LSSEntryFinger> b = new AtomicReferenceArray<LSSEntryFinger>(numBucket);
			for(int j=0;j<numBucket;j++){
				int offsetBucket = j*bucketByteSize;
				temp = ByteArrays.getSubArray(tempLSS, offsetArray+offsetBucket,bucketByteSize);
				
				int[] val = LSSFingerprintAtomicCBF.ReverseCounterAndSum(temp);
				LSSEntryFinger a = new LSSEntryFinger(val);
				b.set(j, a);
			}
			lsstable.set(i, b);
		}
		return lsstable;
	}

	/**
	 * bf in bytes
	 * @param clusterGrouper2
	 * @return
	 */
	private byte[] BF2Bytes(AtomicReferenceArray<CountingBloomFilter> clusterGrouper2) {
		// TODO Auto-generated method stub
		int oneCBFBytes=clusterGrouper2.get(0).getSize()*ByteArrays.INT_SIZE_IN_BYTES;
		byte[] cbfs = new byte[clusterGrouper2.length()*oneCBFBytes];
		for(int i=0;i< clusterGrouper2.length();i++){
			 
			CountingBloomFilter cbf = clusterGrouper2.get(i);
			 byte[] cbfOneByte = cbf.toBytes();
			 System.arraycopy(cbfOneByte, 0, cbfs, i*oneCBFBytes,oneCBFBytes);			
		}
		//numHash
		int NumHash = clusterGrouper2.get(0).nbHash;
		return  ByteArrays.concatenate(ByteArrays.toByteArray(NumHash),cbfs);
	}



	/**
	 * float to bytes
	 * @param clusterCenters2
	 * @return
	 */
	private byte[] Float2Bytes(AtomicReferenceArray<Float> clusterCenters2) {
		// TODO Auto-generated method stub
		float[] clus = new float[clusterCenters2.length()];
		for(int i=0;i<clus.length;i++){
			clus[i] = clusterCenters2.get(i);
		}
		
		return Float2Bytes(clus);
	}



	/**
	 * get table size
	 * @return
	 */
	public long length() {
		// TODO Auto-generated method stub
		return distinctFlows();
	}

	
	

	/**
	 * insert
	 * @param kv
	 */
	public void insert(KVTable kv) {
		// TODO Auto-generated method stub
		for(Entry<Key, Integer> entry: kv.fullRecords.entrySet()){
			this.insert(entry.getKey(), entry.getValue());
		}	
	}

	/**
	 * 
	 * @param bytes
	 */
	public void insert(byte[] bytes) {
		// TODO Auto-generated method stub
		byte[] key = Pub4PCapStreamKVTable.getKey4KVBytes(bytes);
		int val = Pub4PCapStreamKVTable.getValue4KVBytes(bytes);
		insert(key, val);
		
	}

	public void resetShadow(){
		try {
			boolean isLockAcquired = lock.tryLock(100, TimeUnit.MILLISECONDS);
			
			if(isLockAcquired ){
				 
				 try{ 
					 shadowMapGlobal.clear();
				 }finally{
					 lock.unlock();
				 }
					 }
							
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	}
	
	public void resetCBF() {
		// TODO Auto-generated method stub
		for(int i=0;i<this.clusterCount;i++){
			CountingBloomFilter cbf = this.clusterGrouper.get(i);
			cbf.reset();
		}
	}

	public void resetBucketArray() {
		// TODO Auto-generated method stub
		for(int i=0;i<this.clusterCount;i++){
			AtomicReferenceArray<LSSEntryFinger> ba = this.LSSTable.get(i);
			for(int j=0;j<ba.length();j++){
				LSSEntryFinger xx = ba.get(j);
				xx.clear();
			}
		}
	}

	
	
}
