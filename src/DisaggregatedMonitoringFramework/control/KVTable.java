package DisaggregatedMonitoringFramework.control;


import java.util.LinkedHashMap;
import java.util.Map;
 

import com.google.common.collect.Maps;

import edu.harvard.syrah.prp.ByteArray;
import util.bloom.Apache.Key;

public class KVTable {

	/**
	 * field
	 */
	public LinkedHashMap<Key,Integer> fullRecords = new LinkedHashMap<Key,Integer>();
	
	public KVTable(){
		//fullRecords = Maps.newLinkedHashMap();
		
	}
	
	public KVTable(Map<Key, Integer> buffMap) {
		// TODO Auto-generated constructor stub
		fullRecords = new LinkedHashMap<Key,Integer>();
		fullRecords.putAll(buffMap);
	}

	public void put(Key k,Integer i){
		fullRecords.put(k, i);
	}
	public int get(Key k){
		return fullRecords.get(k);
	}

	public long size() {
		// TODO Auto-generated method stub
		return fullRecords.size();
	}

	public Map<Key, Integer> getFullRecords() {
		return fullRecords;
	}

	public void setFullRecords(LinkedHashMap<Key, Integer> fullRecords) {
		this.fullRecords = fullRecords;
	}
	
	
}
