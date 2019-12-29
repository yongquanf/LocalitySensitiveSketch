package util.bloom.Apache;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;

import edu.harvard.syrah.prp.Log;

import util.async.Util;
import util.bloom.Apache.Hash.Hash;


/**
 * partition hash
 * sigmetrics 2007
 * @author Administrator
 *
 */
public class PartitionHashingBF {

	//hash
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
	  
	static Log log=new Log( PartitionHashingBF.class);
	/**
	 * keys in each group
	 */
	public Hashtable<Integer,Set<Key>> groupIndexForKeys =new 
	Hashtable<Integer,Set<Key>>();
	/**
	 * hash function index for each group
	 */
	public Hashtable<Integer,Set<Integer>> HashIndexForGroups =new 
	Hashtable<Integer,Set<Integer>>();
	
	/**
	 * current best filled factor
	 */
	public Hashtable<Integer,Double> FilledFactorForGrous=new
	Hashtable<Integer,Double>();
	
	//parameters
	int g=100;
	int H=100;
	
	int m=8000;
	int k=10;
	
	/**
	 * to simulate the insert/delete
	 */
	CountingBloomFilter CBF;
	/**
	 * final bf
	 */
	BloomFilter bf;
	
	/**
	 * random generator
	 */
	Random r= new Random(System.currentTimeMillis());
	
	/**
	 * constructor
	 * @param g_
	 * @param H_
	 * @param m_
	 * @param k_
	 */
	public PartitionHashingBF(int g_,int H_,int m_,int k_){
		g=g_;
		H=H_;
		m=m_;
		k=k_;
		initBF();
	}
	
	/**
	 * init the bf
	 */
	private void initBF(){
		CBF=new CountingBloomFilter(m,k,Hash.MURMUR_HASH);
		
		bf=new BloomFilter(m,k,Hash.MURMUR_HASH);
	}
	
	/**
	 * construct the groups
	 * @param s
	 */
	public void constructGroups(Set<Key> s){
		Iterator<Key> ier = s.iterator();
		while(ier.hasNext()){
			Key t = ier.next();
			
			int idx=RS(t,g);
			
			Set<Key> ks;
			if(!groupIndexForKeys.containsKey(idx)){
				ks=new HashSet<Key>(2);
			}else{
				ks=groupIndexForKeys.get(idx);
			}
			ks.add(t);
			groupIndexForKeys.put(idx, ks);
			
		}
	}
	
	/**
	 * find best hash functions
	 */
	public void findBestHashes(){
		
		Step1ChooseHash();
	
		
		log.info("fp: "+CBF.getPosteriorFP());
	
		double fp2=getPosteriorFP();
		double nowFP;
		
		while(true){
			Step2Iteration();
			nowFP=CBF.getPosteriorFP();
			
			if(nowFP>fp2){
				break;
			}else{
				fp2=nowFP;
			}
			log.info("fp: "+nowFP);
		}
		
		/*
		int repts=4;
		for(int i=0;i<repts;i++){
			//log.info("repeat: "+i);
			Step2Iteration();	
			
			log.info("fp: "+Math.pow(
					CBF.getPosteriorFP(),k));
		}*/
				
	}
	
	/**
	 * construct the Bloom filter
	 */
	public void constructBFUsingBestHashes(){
		
		Iterator<Entry<Integer, Set<Key>>> ierG = groupIndexForKeys.entrySet().iterator();
		while(ierG.hasNext()){
			Entry<Integer, Set<Key>> cG = ierG.next();
			int group=cG.getKey();
			
			Set<Key> keys = cG.getValue();
			//each hash
			
			Set<Integer> hashes = HashIndexForGroups.get(group);
			
			Iterator<Key> ier = keys.iterator();
			while(ier.hasNext()){
				Key tmp = ier.next();
				
				Iterator<Integer> ierH = hashes.iterator();
				while(ierH.hasNext()){
					Integer hh = ierH.next();
					bf.setBit(h4Partitioned(tmp,hh,m));
				}
				
			}
			
			
/*			for(int i=0;i<keys.size();i++){
				Key key = keys.get(i);
				
				for(int j=0;j<hashes.size();j++){
					Integer hh = hashes.get(j);
					
					bf.setBit(h(key,hh,m));
					
					
				}
				
			}*/
			
			
			/*Iterator<Integer> ierH = (HashIndexForGroups.get(group)).iterator();
			
			while(ierH.hasNext()){				
				Integer j = ierH.next();
				
				Iterator<Key> ierK = keys.iterator();
				while(ierK.hasNext()){
					Key curKey = ierK.next();					
					int indKey=h(curKey,j,m);
					bf.setBit(indKey);					
				}				
			}*/		
		}	
	}
	
	
	/**
	 * posterior fp
	 * @return
	 */
	public double getPosteriorFP(){
		double ff=bf.filledFactor();
		return Math.pow(ff, k);	
	}
	
	
	/**
	 * choose hash
	 */
	public void Step1ChooseHash(){
		Iterator<Entry<Integer, Set<Key>>> ier = 
			groupIndexForKeys.entrySet().iterator();
		
		//int totalGroups=groupIndexForKeys.size();
		int finished=-1;
		
		while(ier.hasNext()){
			Entry<Integer, Set<Key>> im = ier.next();
			
			finished++;
			//group and keys
			int group=im.getKey();			
			Set<Key> keys = im.getValue();
			
			//log.info("complete: "+(finished+0.0)/totalGroups);
			
			//empty group
			if(keys.isEmpty()){
				continue;
			}
			
			//select k hash functions
			for(int rept=0;rept<k;rept++){
			log.debug("repeat: "+rept);
				
				
			//for current group, select hash functions		
			double minFilledFactor=200;
			int curIndexH=-1;
			int ih;
			
			Set<Integer> pos=new HashSet<Integer>();
			//iterate current hash function
			for(int ihash=1;ihash<H+1;ihash++){
				//log.info("# hash: "+ih)
				
				//random hash function
				ih=r.nextInt(H)+1;
				//int setted=0;
				
				pos.clear();
				//add all keys
				Iterator<Key> ierK = keys.iterator();
				while(ierK.hasNext()){
					Key curKey = ierK.next();					
					int indKey=h4Partitioned(curKey,ih,m);
					//System.out.print(" "+indKey);
					//log.info("$: "+indKey);
					pos.add(indKey);
				}
				
				CBF.setBit(pos);	
				
				
				
				//log.info("set "+setted);
				
				//count filled factor
				double ff=CBF.filledFactor();
				if(ff<minFilledFactor){
					minFilledFactor=ff;
					curIndexH=ih;
				}
				//delete
				//setted=0;
				
				
				CBF.deleteBit(pos);
				
//				ierK = keys.iterator();
//				while(ierK.hasNext()){
//					Key curKey = ierK.next();					
//					int indKey=h(curKey,ih,m);
//					//log.info("$: "+indKey);
//					CBF.deleteBit(indKey);
//					setted++;
//				}			
				//log.debug("removed "+setted);
			}//end select hash functions
			
			Set<Integer> hashKeys;
			
			if(!HashIndexForGroups.containsKey(group)){
				hashKeys = new HashSet<Integer>();				
			}else{
				hashKeys=HashIndexForGroups.get(group);
			}
			
			//add a hash
			hashKeys.add(curIndexH);
			
			log.debug("# hash: "+hashKeys.size());
			
			HashIndexForGroups.put(group, hashKeys);
			//add the filled factor
			
			
			
			//Set<Double> ls1 = FilledFactorForGrous.get(group);
			//ls1.add( minFilledFactor);
			FilledFactorForGrous.put(group,  minFilledFactor);
			
			log.debug("filled factor: "+minFilledFactor);
			//hash keys into cbf
			//int setted=0;
			Iterator<Key> ierK = keys.iterator();
			while(ierK.hasNext()){
				Key curKey = ierK.next();					
				int indKey=h4Partitioned(curKey,curIndexH,m);
				CBF.setBit(indKey);	
				//setted++;
			}						
			//log.info("set "+setted);
			
			}//select k hash functions
			
		}//current group		
	}
	
	
	/**
	 * iterate to select best hashes
	 */
	public void Step2Iteration(){
		
		Iterator<Entry<Integer, Set<Key>>> ier = groupIndexForKeys.entrySet().iterator();
		while(ier.hasNext()){
			Entry<Integer, Set<Key>> im = ier.next();
			
			//current group
			int group=im.getKey();
			Set<Key> keys = im.getValue();
		
			//clear 
			Set<Integer> hashes = HashIndexForGroups.get(group);
			Iterator<Integer> ierH = hashes.iterator();
			while(ierH.hasNext()){
				Integer curH = ierH.next();				
				Iterator<Key> ierKeys = keys.iterator();
				while(ierKeys.hasNext()){
					Key curKey = ierKeys.next();
					int indKey=h4Partitioned(curKey,curH,m);
					CBF.deleteBit(indKey);				
				}//end key								
			}//end hash
			
			//cache current hash functions
			Set<Integer> cachedHashes = new HashSet<Integer>(
					HashIndexForGroups.get(group)		
			);
			//cache current filled factor
			 double currentFilledFactor =FilledFactorForGrous.get(group);
				 
			 //current filled factor 
			 double pFF1=currentFilledFactor;
			
			//release
			HashIndexForGroups.get(group).clear();
			HashIndexForGroups.remove(group);
			
			
			FilledFactorForGrous.remove(group);
			
			//reselect hash		
			for(int rept=0;rept<k;rept++){
				
				//for current group, select hash functions		
				double minFilledFactor=100;
				int curIndexH=-1;
				int ih;
				
				Set<Integer> pos=new HashSet<Integer>();
				for(int ihash=1;ihash<H+1;ihash++){
					
					ih=r.nextInt(H)+1;
					//add all keys
					
					pos.clear();
					//add all keys
					Iterator<Key> ierK = keys.iterator();
					while(ierK.hasNext()){
						Key curKey = ierK.next();					
						int indKey=h4Partitioned(curKey,ih,m);
						//System.out.print(" "+indKey);
						//log.info("$: "+indKey);
						pos.add(indKey);
					}
					
					CBF.setBit(pos);	
					
					
					
					//count filled factor
					double ff=CBF.filledFactor();
					if(ff<minFilledFactor){
						minFilledFactor=ff;
						curIndexH=ih;
					}
					//delete					
					CBF.deleteBit(pos);	
									
				}//end select hash functions
				
				Set<Integer> hashKeys;
				
				if(!HashIndexForGroups.containsKey(group)){
					hashKeys = new HashSet<Integer>();				
				}else{
					hashKeys=HashIndexForGroups.get(group);
				}
				
				//add a hash
				hashKeys.add(curIndexH);
				HashIndexForGroups.put(group, hashKeys);
				//add the filled factor
				
				
				FilledFactorForGrous.put(group, minFilledFactor);
				
				//hash keys into cbf
				Iterator<Key> ierK = keys.iterator();
				while(ierK.hasNext()){
					Key curKey = ierK.next();					
					int indKey=h4Partitioned(curKey,curIndexH,m);
					CBF.setBit(indKey);					
				}	
				
				}//select k hash functions
			
			double pFF2= FilledFactorForGrous.get(group);
			if(pFF2>pFF1){
				//fallback
				HashIndexForGroups.get(group).clear();
				HashIndexForGroups.remove(group);
				
				FilledFactorForGrous.remove(group);
				
				//add original
				HashIndexForGroups.put(group, new HashSet<Integer>(cachedHashes));
				
				FilledFactorForGrous.put(group,currentFilledFactor);
			}
			
			//release space
			
			cachedHashes.clear();
			cachedHashes=null;
						
		}//each group
		
	}
	
	

	
	/**
	 * first stage
	 * @param s
	 * @param g
	 * @return
	 */
	public int RS(Key s,int g){
		
		return (int)GeneralHashFunction.RSHash(Integer.toString(
				ByteBuffer.wrap(s.bytes).getInt()), g);
		
	}
	
	/**
	 * constant for CRC
	 */
	static int R1=0x5ed50e23;
	static int R2=0x1b75e0d1;
	/**
	 * second state
	 * @param s
	 * @param index
	 * @return
	 */
	public static int h2(Key s,int index,int BFLen){
		
		int keyVal = ByteBuffer.wrap(s.bytes).getInt();
		
		int v1=CRC32.getInstance().getValue(Integer.toString(0),keyVal);
		
		int v2=CRC32.getInstance().getValue(Integer.toString(v1), 
				R1+index*R2);
		
		v1=Math.abs(v2 % BFLen);
		return v1;
		
	}
	
	/**
	 * compute the hash
	 * @param s
	 * @param index
	 * @param BFLen
	 * @return
	 */
	public static int h5(Key s,int index,int BFLen){
	
		HashFunction hash = new HashFunction(BFLen, 2,Hash.MURMUR_HASH, 1);
	
		int[] hs = hash.hash(s);
		
		int fi=(int)Math.round(Math.pow(index,2));
		
		return Math.abs((hs[0]+hs[1]*fi)%BFLen);
	}
	
	/**
	 * md5 based
	 * @param s
	 * @param index
	 * @param BFLen
	 * @return
	 */
	public static int h0(Key s,int index,int BFLen){
	
	byte[] b = s.getBytes();
	int l=0;	
	int nbHash=2;
	int[]  result={1,1};
	int maxValue=BFLen;
	
	digestFunction.reset();  
	int salt=0;
	
	while (l < nbHash) {
        byte[] digest;
        synchronized (digestFunction) {
            digestFunction.update(ByteBuffer.allocate(4).putInt((R1+R2*salt)).array());
            salt++;
           // salt=hashFunction.hash(digestFunctionSeed.digest(ByteBuffer.allocate(4).putInt((R1+R2*salt)).array()));
            
            digest = digestFunction.digest(b);                
        }
    
        for (int i = 0; i < digest.length/4 && l < nbHash; i++) {
            int hvalues = 0;
            for (int j = (i*4); j < (i*4)+4; j++) {
                hvalues <<= 8;
                hvalues |= ((int) digest[j]) & 0xFF;
            }
            result[l] =  Math.abs(hvalues % maxValue);
            l++;
        }
    } 
    
	int fi=(int)Math.round(Math.pow(index,2));
	
	return Math.abs((result[0]+result[1]*fi)%BFLen);
}
	
	/**
	 * compute one hash, indexed by the salt
	 * @param s
	 * @param KPerGroup
	 * @param indexHash
	 * @param BFLen
	 * @return
	 */
	public static int h4Partitioned(Key s,int indexHash,int BFLen){

		int KPerGroup=1;
		
		byte[] b = s.getBytes();
				
		digestFunction.reset();  
		
		int[] result= new int[KPerGroup];
		
		//first index of the salt
		int salt=indexHash*KPerGroup;
		
		int l=0;
		
		while (l < KPerGroup) {
	        byte[] digest;
	        
	        synchronized (digestFunction) {
	            digestFunction.update(ByteBuffer.allocate(4).putInt((R1+R2*salt)).array());
	            salt++;
	           // salt=hashFunction.hash(digestFunctionSeed.digest(ByteBuffer.allocate(4).putInt((R1+R2*salt)).array()));
	            
	            digest = digestFunction.digest(b);                
	        }
	    
	        for (int i = 0; i < digest.length/4 && l < KPerGroup; i++) {
	            int hvalues = 0;
	            for (int j = (i*4); j < (i*4)+4; j++) {
	                hvalues <<= 8;
	                hvalues |= ((int) digest[j]) & 0xFF;
	            }
	            result[l] =  Math.abs(hvalues % BFLen);
	            l++;
	        }
	    } 
	    		
		return result[0];
	}
	
	
	/**
	 * compute a group of hash function values
	 * @param s
	 * @param KPerGroup
	 * @param indexGroup
	 * @param BFLen
	 * @return
	 */
	public static int[] h(Key s,int KPerGroup,int indexGroup,int BFLen){

		
		byte[] b = s.getBytes();
				
		digestFunction.reset();  
		
		int[] result= new int[KPerGroup];
		
		//first index of the salt
		int salt=indexGroup*KPerGroup;
		
		int l=0;
		
		while (l < KPerGroup) {
	        byte[] digest;
	        
	        synchronized (digestFunction) {
	            digestFunction.update(ByteBuffer.allocate(4).putInt((R1+R2*salt)).array());
	            salt++;
	           // salt=hashFunction.hash(digestFunctionSeed.digest(ByteBuffer.allocate(4).putInt((R1+R2*salt)).array()));
	            
	            digest = digestFunction.digest(b);                
	        }
	    
	        for (int i = 0; i < digest.length/4 && l < KPerGroup; i++) {
	            int hvalues = 0;
	            for (int j = (i*4); j < (i*4)+4; j++) {
	                hvalues <<= 8;
	                hvalues |= ((int) digest[j]) & 0xFF;
	            }
	            result[l] =  Math.abs(hvalues % BFLen);
	            l++;
	        }
	    } 
	    		
		return result;
	}
	
	
	/**
	 * hash function
	 * @param s
	 * @param index
	 * @param BFLen
	 * @return
	 */
	public static int h4(Key s,int index,int BFLen){
		/**
		 * get a hash
		 */
		Hash oneHash=Hash.getInstance(Hash.MURMUR_HASH);
		
		Hash SecondHash=Hash.getInstance(Hash.JENKINS_HASH);
		
		int init=0;
		init=oneHash.hash(s.getBytes(),init);
		
		//hash use the default seed
		int hashA = Math.abs(init%BFLen);
		//use the first value as the seed
		int hashB = Math.abs(SecondHash.hash(s.getBytes(),init)%BFLen);
		
		//m even, then hashB odd,
		if(BFLen%2==0){
			hashB=2*hashB+1;
		}
		int fi=(int)Math.round(Math.pow(index,2));
		return (hashA+fi*hashB)%BFLen;		
	}
	
	/**
	 * compute the intersection 
	 * @return
	 */
	public BloomFilter intersect(PartitionHashingBF bf2){
		BloomFilter cpBF = this.bf.getCopy();
		cpBF.and(bf2.bf);
		
		return cpBF;
		
	}
	
	/**
	 * average query time
	 * @param keys
	 * @return
	 */
	public double getQueryTime(Collection<Key> keys){
		
		int n=keys.size();
		
		double sumTime=0;
		
		Iterator<Key> ier = keys.iterator();
		
		Key y;
		
		long start;
		while(ier.hasNext()){
			
			y=ier.next();
			
			start=System.currentTimeMillis();
			membershipTest(y);
			
			sumTime+=(System.currentTimeMillis()-start+0.0)/1000;
		}
		
		return sumTime/n;		
	}

	public boolean membershipTest(Key key) {
		// TODO Auto-generated method stub
		
		//select the group;
		int idx=RS(key,g);
		//choose hash functions
		Set<Integer> hashes = HashIndexForGroups.get(idx);
		
		boolean isIn=true;
		//query the bit
		
		Iterator<Integer> ierH = hashes.iterator();
		while(ierH.hasNext()){
			int hashF=ierH.next();
			if(!bf.queryBit(h4Partitioned(key,hashF,m))){
				isIn=false;
				break;
			}
		}
		
		
/*		for(int i=0;i<hashes.size();i++){
			int hashF=hashes.get(i);	
			if(!bf.queryBit(h(key,hashF,m))){
				isIn=false;
				break;
			}
		}//query the bit
*/		
		return  isIn;
		
	}
	
	/**
	 * test whether the key is in the set
	 * @param key
	 * @param bFIntersect
	 * @return
	 */
	public boolean membershipTest(Key key, BloomFilter bFIntersect) {
		// TODO Auto-generated method stub
		
		//select the group;
		int idx=RS(key,g);
		//choose hash functions
		Set<Integer> hashes = HashIndexForGroups.get(idx);
		
		boolean isIn=true;
		//query the bit
		
		Iterator<Integer> ierH = hashes.iterator();
		while(ierH.hasNext()){
			Integer hashF = ierH.next();
			if(!bFIntersect.queryBit(h4Partitioned(key,hashF,m))){
				isIn=false;
				break;
			}
		}
		
/*		for(int i=0;i<hashes.size();i++){
			int hashF=hashes.get(i);	
			if(!bFIntersect.queryBit(h(key,hashF,m))){
				isIn=false;
				break;
			}
		}//query the bit
*/		
		return  isIn;
		
	}

	/**
	 * clear the structure
	 */
	public void clear() {
		// TODO Auto-generated method stub
		Iterator<Set<Key>> ier = groupIndexForKeys.values().iterator();
		while(ier.hasNext()){
			ier.next().clear();
		}
		groupIndexForKeys.clear();
		groupIndexForKeys=null;
		
		Iterator<Set<Integer>> ierH = HashIndexForGroups.values().iterator();
		while(ierH.hasNext()){
			ierH.next().clear();
		}
		HashIndexForGroups.clear();
		HashIndexForGroups=null;
		
		FilledFactorForGrous.clear();
		
		CBF.clear();
		CBF=null;
		
		bf.clear();
		bf=null;
		
	}

	/**
	 * query time
	 * @param nodeA_Neighbor
	 * @return
	 */
	public double getAddTime(Set<Key> nodeA_Neighbor) {
		// TODO Auto-generated method stub
int n=nodeA_Neighbor.size();
		
		double sumTime=0;
		
		Iterator<Key> ier = nodeA_Neighbor.iterator();
		
		Key y;
		
		long start;
		while(ier.hasNext()){
			
			y=ier.next();
			
			start=System.currentTimeMillis();
			membershipTest(y);
			
			sumTime+=(System.currentTimeMillis()-start+0.0)/1000;
		}
		
		return sumTime/n;	
	}
	
	
}
