package util.bloom.Cuckoo;

import org.apache.commons.lang.SerializationUtils;

import com.google.common.hash.Funnels;

import util.bloom.Cuckoo.Utils.Algorithm;

public class ByteCFExchange {

	static ByteCFExchange instance=null;
	/**
	 * singleton
	 * @return
	 */
	public static ByteCFExchange getInstance(){
		if(instance==null){
			instance = new ByteCFExchange();
		}
		return instance;
	}
	
	/**
	 * cuckoo with long as entries
	 * @param maxElements
	 * @param fp
	 * @return
	 */
	public CuckooFilter<Long> createLongKeyCuckooFilter(int maxElements, float fp){
		CuckooFilter<Long> filter = new CuckooFilter.Builder<>(Funnels.longFunnel(),maxElements).withFalsePositiveRate(fp).
				withHashAlgorithm(Algorithm.Murmur3_128).build();		
		return filter;
	}
	
	public CuckooFilter<Integer> createIntegerKeyCuckooFilter(int maxElements, float fp){
		CuckooFilter<Integer> filter = new CuckooFilter.Builder<>(Funnels.integerFunnel(),maxElements).withFalsePositiveRate(fp).
				withHashAlgorithm(Algorithm.Murmur3_32).build();		
		return filter;
	}
	
	/**
	 * serialize
	 * @param cf
	 * @return
	 */
	public byte[] serialize2ByteArray(CuckooFilter<byte[]> cf){		
		return SerializationUtils.serialize(cf);
	}
	
	@SuppressWarnings("unchecked")
	public CuckooFilter<byte[]> reverseCuckooFilter(byte[] cfArray){
		return (CuckooFilter<byte[]>) SerializationUtils.deserialize(cfArray);
	}

	public CuckooFilter<byte[]> createKeyCuckooFilter(int expectedNumItems, float fp) {
		// TODO Auto-generated method stub
		CuckooFilter<byte[]> filter = new CuckooFilter.Builder<>(Funnels.byteArrayFunnel(),expectedNumItems).withFalsePositiveRate(fp).
				withHashAlgorithm(Algorithm.Murmur3_128).build();		
		return filter;
	}
	
}
