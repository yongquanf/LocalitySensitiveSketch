package DisaggregatedMonitoringFramework.Query;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.common.io.ByteStreams;

import DisaggregatedMonitoringFramework.Sketching.LSSFingerprintAtomic;
import ECS.FingerprintTableSlot.ItemInfo;
import util.bloom.Apache.Key;

/**
 * async flow query
 * @author quanyongf
 *
 */
public class AsyncFlowApps {

	public AsyncFlowApps(){
		
	}
	
	
	
	public long parseOnlineTables(Map<Key,Integer> flowTable,LSSFingerprintAtomic lss){
		long t1 = System.currentTimeMillis();
		for(Entry<Key,Integer> entry: flowTable.entrySet()){
			lss.insert(entry.getKey(), entry.getValue());
		}
		long t2 = System.currentTimeMillis();
		return (t2-t1);
	}
	
	/**
	 * build an empty table
	 * @param flowTable
	 * @return
	 */
	public Map<Key,Double> buildEmptyTable(Map<Key,Integer> flowTable){
		HashMap<Key,Double> table = new HashMap<Key,Double>();
		for(Entry<Key,Integer> entry: flowTable.entrySet()){
			table.put(entry.getKey(), 0.0);
		}
		return table;
	}
	
	/**
	 * 
	 * @param key
	 * @param lss
	 * @return
	 */
	public double perFlowFrequency(Key key, LSSFingerprintAtomic lss){
		return lss.query(key)[0];
	}
	
	
	
	public float flowSizeDistribution(Map<Key,Double> kvEmpty,LSSFingerprintAtomic lss){
		//long t1 = System.currentTimeMillis();
		return lss.CollectEstimatedSize(kvEmpty);
		//long t2 = System.currentTimeMillis();
		//return (t2-t1);
	}
	
	/**
	 * estimte entropy, need collected kv pairs from LSSFingerprintAtomic as input
	 */
	public double FlowEntropy(Map<Key,Double> kvData){
		double sum = 0;
		double tmp = 0;
		for(Entry<Key, Double> entry: kvData.entrySet()){
			tmp = entry.getValue();
			if(tmp<=0){continue;}
			sum -= tmp*Math.log(tmp);
		}
		return sum;
	}
	
	public double FlowEntropyInteger(Map<Key,Integer> kvData){
		double sum = 0;
		double tmp = 0;
		for(Entry<Key, Integer> entry: kvData.entrySet()){
			tmp = entry.getValue();
			if(tmp<=0){continue;}
			sum -= tmp*Math.log(tmp);
		}
		return sum;
	}
	
	/**
	 * hh,  need collected kv pairs from LSSFingerprintAtomic as input
	 * @param data
	 * @param threshold
	 * @return
	 */
	public Map<Key,Double> heavyHitter(Map<Key,Double> data,double threshold){
		 Map<Key,Double> output = new HashMap<Key,Double>();
	        for (Map.Entry<Key, Double> entry : data.entrySet()) {
	            if (entry.getValue() > threshold) {
	                output.put(entry.getKey(), entry.getValue());
	            }
	        }
	        return output;
	}
	
	public Map<Key,Double> heavyHitterInteger(Map<Key,Integer> data,double threshold){
		 Map<Key,Double> output = new HashMap<Key,Double>();
	        for (Map.Entry<Key, Integer> entry : data.entrySet()) {
	            if (entry.getValue() > threshold) {
	                output.put(entry.getKey(), entry.getValue()+0.0);
	            }
	        }
	        return output;
	}
	
	
	
	public int distincFlows(LSSFingerprintAtomic lss){
		return lss.distinctFlows();
	}
	
	
	/**
	 * compare adjacent data, need collected kv pairs from LSSFingerprintAtomic as input
	 * @param data
	 */
	public ArrayList<HashMap<Key,Double>> heavyChange(List<Map<Key,Double>> data, double threshold){
		int len = data.size();
		int left = 0;
		int right = left +1;
		Key tmp;
		
		ArrayList<HashMap<Key,Double>> outTable = new ArrayList<HashMap<Key,Double>>();
		
		for(int i=right;i<len;i++){
			Map<Key,Double> dA = data.get(left);
			Map<Key,Double> dB = data.get(right);
			
			HashMap<Key,Double> out = new HashMap<Key,Double>();
			
			SetView<Key> commons = Sets.intersection(dA.keySet(), dB.keySet()); 
			if(!commons.isEmpty()){
			Iterator<Key> ier = commons.iterator();
			//iterator
			while(ier.hasNext()){
				 tmp = ier.next();
				 double val = Math.abs(dA.get(tmp) - dB.get(tmp));
				 //add a new record
				 if(val> threshold){
					 out.put(tmp, val);
				 }
			}//end
			
			outTable.add(out);
			
			}//end current
			//update
			left = right;
			right ++;
		}//end
		return outTable;
	}
	
	
	/**
	 * compare two map
	 * @param left
	 * @param right
	 * @param threshold
	 * @return
	 */
	public HashMap<Key,Double> heavyChange(Map<Key,Double> dA, Map<Key,Double> dB, double threshold){
		
		Key tmp;
			
			HashMap<Key,Double> out = new HashMap<Key,Double>();
			
			SetView<Key> commons = Sets.intersection(dA.keySet(), dB.keySet()); 
			if(!commons.isEmpty()){
			Iterator<Key> ier = commons.iterator();
			//iterator
			while(ier.hasNext()){
				 tmp = ier.next();
				 double val = Math.abs(dA.get(tmp) - dB.get(tmp));
				 //add a new record
				 if(val> threshold){
					 out.put(tmp, val);
				 }
			}//end
			}//end current
			return out;		
	}

	/**
	 * heavy change
	 * @param dA
	 * @param dB
	 * @param threshold
	 * @return
	 */
	public HashMap<Key,Double> heavyChangeInteger(Map<Key,Integer> dA, Map<Key,Integer> dB, double threshold){
		
		Key tmp;
			
			HashMap<Key,Double> out = new HashMap<Key,Double>();
			
			SetView<Key> commons = Sets.intersection(dA.keySet(), dB.keySet()); 
			if(!commons.isEmpty()){
			Iterator<Key> ier = commons.iterator();
			//iterator
			while(ier.hasNext()){
				 tmp = ier.next();
				 double val = Math.abs(dA.get(tmp) - dB.get(tmp));
				 //add a new record
				 if(val> threshold){
					 out.put(tmp, val);
				 }
			}//end
			}//end current
			return out;		
	}

	
	/**
	 * parse
	 * @param onlineFile
	 * @param lss
	 * @return
	 * @throws FileNotFoundException 
	 */
	public long parseOnlineTables(String onlineFile, LSSFingerprintAtomic lss) throws Exception {
		// TODO Auto-generated method stub
		
		InputStream is;

		is = new BufferedInputStream(new FileInputStream(onlineFile));
		//
		int headerLen = 13;
		int fourByte = 4;
		byte[] bufferHeader = new byte[headerLen];
		
		int bytesNum = headerLen;
		int count=0;
		Key  str;
		 
		long t1 = System.currentTimeMillis();
		count = ByteStreams.read(is, bufferHeader,0,bufferHeader.length);
		
		//count=is.read(bufferHeader);
		while(count == bytesNum){
			//byte
			 //str = new String(bufferHeader);
			
			//first four bytes
			byte[] source = new byte[4];
			System.arraycopy(bufferHeader, 0, source, 0, fourByte);
			str = new Key(source);
			
			//insert
			lss.insert(str, 1);
			
			
			//System.out.println(POut.toString(buffer_13));
			//read next
			//count=is.read(bufferHeader);
			count = ByteStreams.read(is, bufferHeader,0,bufferHeader.length);
		}
		long t2 = System.currentTimeMillis();
		is.close();
		
		return (t2-t1);
	}
	
	
	
}
