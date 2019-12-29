package util.bloom.Cuckoo;

import com.google.common.hash.Funnels;

import util.bloom.Cuckoo.Utils.Algorithm;

public class Example {

	public static void main(String[] args) {
		// create
		CuckooFilter<Integer> filter = new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_128).build();
		// insert
		if (filter.put(42)) {
			System.out.println("Insert Success!");
		}
		// contains
		if (filter.mightContain(42)) {
			System.out.println("Found 42!");
		}
		// count
		System.out.println("Filter has " + filter.getCount() + " items");
		
				// count
		System.out.println("42 has been inserted approximately " + filter.approximateCount(42) + " times");

		// % loaded
		System.out.println("Filter is " + String.format("%.0f%%", filter.getLoadFactor() * 100) + " loaded");

		// delete
		if (filter.delete(42)) {
			System.out.println("Delete Success!");
		}
	
	}
}
