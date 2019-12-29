package DisaggregatedMonitoringFramework.Query;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;

import DisaggregatedMonitoringFramework.Sketching.LSSFingerprintAtomic;
import ECS.FlowApps;
import ECS.LSSFingerprint;
import avro.shaded.com.google.common.collect.MapDifference;
import avro.shaded.com.google.common.collect.Maps;
import ECS.FingerprintTableSlot.ItemInfo;
import edu.harvard.syrah.prp.POut;
import util.bloom.Apache.Key;


public class QueryLSSFingerFlowApps {

	private static final Logger log = LoggerFactory.getLogger(QueryLSSFingerFlowApps.class);
	
	/**
	 * lock for heavy change test
	 */
	//ReentrantLock lock=null;
	
	//test
	AsyncFlowApps test;
	//LSS
	//LSSFingerprint lss;
	//fingerprint ft;
	// FingerprintTableSlot ft;
	
	BufferedWriter bufferedWriter;
	
	//List<Map<Key,Double>> TrueFlowCache = null;
	//List<Map<Key,Double>> EstFlowCache = null;
	
	
	/**
	 * cache
	 * @author quanyongf
	 *
	 */
	class MapPair{
		Map<Key, Integer> trueTable;
		Map<Key,Double> estTable;
		
		public MapPair(Map<Key, Integer> trueFlowTable,Map<Key,Double> _estCache){
			trueTable = trueFlowTable;
			estTable = _estCache;
		}
		
	}
	/**
	 * cache
	 */
	//ConcurrentLinkedQueue<MapPair> xCache; 
	CopyOnWriteArrayList<MapPair> xCache;
	
	
	/**
	 * heavy hitter left
	 */
	//Map<Key,Double>  leftEstLSS=null;
	
	//Map<Key,Integer> leftTrueKVSet = null;
	  
	public QueryLSSFingerFlowApps(String bufferedWriteName){
		test = new AsyncFlowApps();
		
		/**
		 * copy on write
		 */
	
		xCache = Lists.newCopyOnWriteArrayList();
		
		 //xCache = Queues.newConcurrentLinkedQueue();
		//lock = new ReentrantLock();		 
		
		//leftEstLSS = null;
		//leftTrueKVSet = null;		
		try {
			bufferedWriter = new BufferedWriter(new FileWriter(bufferedWriteName,
					true));
			bufferedWriter.write("Trace started\n");
			bufferedWriter.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	} 
	
	/**
	 * main entry, 
	 * CAUTION: fingerprint must be used for inserting items to the sketch,
	 * @param obtainedLSSAtomic2
	 */
	public void process(LSSFingerprintAtomic obtainedLSSAtomic2) {
		// TODO Auto-generated method stub
	
		Map<Key,Integer> trueFlowTable = obtainedLSSAtomic2.getShadowMapGlobal();
		
		//int QueryThreshold = 100;||trueFlowTable.size()<QueryThreshold
		
		
		
		if(trueFlowTable==null||trueFlowTable.isEmpty()){
			log.warn("trueFlowTable is empty! Return!");
			return;
		}else{
			
			//log.info("@@normal! "+trueFlowTable.size());
			entry(trueFlowTable, obtainedLSSAtomic2, bufferedWriter);
			//cache left lss
		}
	}
	
	
	/**
	 * remove empty flow
	 * @param trueFlowTable
	 */
	private void compressFlowTableInteger(Map<Key, Integer> trueFlowTable) {
		// TODO Auto-generated method stub
		
		Iterator<Entry<Key, Integer>> ier = trueFlowTable.entrySet().iterator();
		while(ier.hasNext()){
			Entry<Key, Integer> tmp = ier.next();
			if(tmp.getValue()<=0){
				log.warn("zero entry: "+tmp.getValue());
				ier.remove();
			}
		}
	}

	private void compressFlowTableDouble(Map<Key, Double> trueFlowTable) {
		// TODO Auto-generated method stub
		
		Iterator<Entry<Key, Double>> ier = trueFlowTable.entrySet().iterator();
		while(ier.hasNext()){
			Entry<Key, Double> tmp = ier.next();
			if(tmp.getValue()<=0){
				log.warn("zero entry: "+tmp.getValue());
				ier.remove();
			}
		}
	}
	
	
	/**
	 * 
	 * @param trueTable
	 * @return
	 */
	private Map<Key,Double> FillEstimateTable(Map<Key,Integer> trueTable,
			LSSFingerprintAtomic lss,BufferedWriter bufferedWriter){	
		//init
		Map<Key,Double> EstTable =  test.buildEmptyTable(trueTable);
		//fill 
		float delay = test.flowSizeDistribution(EstTable, lss);
		
		try {
			bufferedWriter.write("LSSQueryAllDelay: "+ trueTable.size()+" " +delay+"\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return EstTable;
	}
	
	/**
	 * main entry
	 * @param onlineFile
	 * @param _n
	 */
	public void entry(Map<Key,Integer> trueFlowTable,LSSFingerprintAtomic lss,BufferedWriter bufferedWriter){
		try {
			
			
			   
			//query starts: 
			Map<Key,Double> estFlowTable = FillEstimateTable(trueFlowTable,lss,bufferedWriter);
			compressFlowTable(estFlowTable );
			compressFlowTableInteger(trueFlowTable);
			//write
			//MapDifference<Key, ? extends Number> x = Maps.difference(trueFlowTable, estFlowTable);
			// bufferedWriter.write("Table: "+ trueFlowTable.size()+" "+estFlowTable.size()+" "
			// +(x.entriesDiffering().toString())+"\n");
			
			
			
			double distictErr = getDistinctStatistics(lss,trueFlowTable);
			
			 double[] flowErr = getFlowDistStatistics(trueFlowTable, estFlowTable,bufferedWriter);
			 
			 double entropyErr = getFlowEntropyStatistics(trueFlowTable, estFlowTable);
			 
			 double threshold = setThreshold(trueFlowTable,90);
			 
			 double hhErr = getHeavyHitterStatistics(trueFlowTable, estFlowTable, threshold,bufferedWriter);
			 
			
			 //obtain the lock 

			 //trueFlowTable.clear();
			 //estFlowTable.clear();
			
			 //xcache contains element
			 
			double hchangeErr = -1;
 
				//add to the current cache
				xCache.add(new MapPair(trueFlowTable,estFlowTable));
				if(xCache.size()>1){
					int siz = xCache.size();
					hchangeErr = getheavyChangeDetect(xCache.get(siz-2),xCache.get(siz-1),threshold);	
					xCache.remove(0);
				}
				//delete top
				
						 
							 //write
		  bufferedWriter.write("result: "+distictErr+" "+POut.toString(flowErr)+
		  " "+ entropyErr+" "+threshold + " "+hhErr+" "+hchangeErr+"\n");						
							 bufferedWriter.flush();		
			 
			 
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * compress
	 * @param estFlowTable
	 */
	private void compressFlowTable(Map<Key, Double> estFlowTable) {
		// TODO Auto-generated method stub
		Iterator<Entry<Key, Double>> ier = estFlowTable.entrySet().iterator();
		while(ier.hasNext()){
			Entry<Key, Double> tmp = ier.next();
			if(tmp.getValue()<=0){
				ier.remove();
			}
		}
	}

	/**
	 * heavy change
	 * @param dA
	 * @param dB
	 * @param dAInteger
	 * @param dBInteger
	 * @param threshold
	 * @return
	 */
	private double getheavyChangeDetect(MapPair x10,MapPair x20,double threshold) {
		// TODO Auto-generated method stub
		
		//MapPair x10 = null;//xCache.poll();
		//MapPair x20 = null;//xCache.peek();
		
		Map<Key,Double> dA = x10.estTable;
		Map<Key,Integer> dAInteger = x10.trueTable;
		
		Map<Key,Double> dB = x20.estTable;
		 Map<Key,Integer> dBInteger= x20.trueTable;
		
		//estimate
		HashMap<Key, Double> x2 = test.heavyChange(dA, dB, threshold);
		//true
		HashMap<Key, Double> x1 = test.heavyChangeInteger(dAInteger, dBInteger, threshold);
		
		if(!x1.isEmpty()){
			double fv = F1Score(x1,x2);
			x1.clear();
			x2.clear();
			return fv;
		}else{
			x1.clear();
			x2.clear();
			return -1;
		}
	}

	/**
	 * 
	 * @param trueFlowTable
	 * @param pct [1,100]
	 * @return
	 */
	private double setThreshold(Map<Key, Integer> trueFlowTable, int pct) {
		// TODO Auto-generated method stub
			 
			 DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
			 for (Integer v : trueFlowTable.values()) {
			     descriptiveStatistics.addValue(v+0.0f);
			 }
			 int pctThreshold = 90;
			 double pct90Val = descriptiveStatistics.getPercentile(pctThreshold);
			 descriptiveStatistics.clear();
			 descriptiveStatistics = null;
			 return (float) pct90Val+0.0f;
		 
	}

	/**
	 * @return median relative error, mean relative error, std, 
	 * @param trueTable
	 * @param EstTable
	 * @param bufferedWriter2 
	 */
	private double[] getFlowDistStatistics(Map<Key,Integer> trueTable, Map<Key,Double> EstTable, BufferedWriter bufferedWriter2){
		
		double[] result=null;
		
		try {
			DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
			DescriptiveStatistics descriptiveStatisticsTrue = new DescriptiveStatistics();
			
			//int len = trueTable.size();
			double ts,es;
		 for (Key entry: trueTable.keySet()) {
			 //not contain
			 if(EstTable.containsKey(entry)==false||trueTable.containsKey(entry)==false){
				 continue;
			 }
			 ts = trueTable.get(entry);
			 es = EstTable.get(entry);
			 if(ts<=0||es<=0){
				 continue;
				 }else{
					 
					 descriptiveStatisticsTrue.addValue(ts);
					 
					 double v = Math.abs(ts - es + 0.0)/ts;
					 descriptiveStatistics.addValue(v);
				 }
		 } 
		 
		 
			bufferedWriter2.write("\nDataStatistics: "+descriptiveStatisticsTrue.getMean()+" "+
					descriptiveStatisticsTrue.getMax()+" "+ 
					descriptiveStatisticsTrue.getMin()+" "+
					descriptiveStatisticsTrue.getStandardDeviation()+" "+
					descriptiveStatisticsTrue.getN()+" "+
					descriptiveStatisticsTrue.getPercentile(50)+" "+
					descriptiveStatisticsTrue.getPercentile(90)+" "+
					descriptiveStatisticsTrue.getPercentile(99)+"\n"
					);
			
			result = new double[]{descriptiveStatistics.getPercentile(50),
					 descriptiveStatistics.getMean(),descriptiveStatistics.getStandardDeviation(),
					 descriptiveStatistics.getVariance(),descriptiveStatistics.getPercentile(90),descriptiveStatistics.getPercentile(99),
					 descriptiveStatistics.getMax(),descriptiveStatistics.getMin()};
			
				descriptiveStatisticsTrue.clear();
				 descriptiveStatistics.clear();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		 
		 
		 return result;
		  
	}
	/**
	 * @return relative error 
	 * @param trueTable
	 * @param EstTable
	 * @return
	 */
	private double getFlowEntropyStatistics(Map<Key,Integer> trueTable, Map<Key,Double> EstTable){
		double te = test.FlowEntropyInteger(trueTable);
		double ee = test.FlowEntropy(EstTable);
		if(te==0){
			return -1;
		}else{
			return -Math.abs(te - ee + 0.0)/te;
		}
	}
	/**
	 * @return F1 score 
	 * @param trueTable
	 * @param EstTable
	 * @param bufferedWriter2 
	 * @return
	 */
	private double getHeavyHitterStatistics(Map<Key,Integer> trueTable, Map<Key,Double> EstTable, double threshold, BufferedWriter bufferedWriter2){
		
		Map<Key, Double> tHH = test.heavyHitterInteger(trueTable, threshold);
		Map<Key, Double>  eHH = test.heavyHitter( EstTable,threshold);
		if(false){
		try {
			bufferedWriter2.write("HHEstimated: "+POut.toString(eHH)+"\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}
		double f1Val = F1Score(tHH, eHH);
		return f1Val;
	}
	/**
	 * relative error
	 * @param trueTable
	 * @param lss
	 * @return
	 */
	private double getDistinctStatistics(LSSFingerprintAtomic lss,Map<Key,Integer> trueTable){
		int eSize = test.distincFlows(lss);
		double rSize = trueTable.size();
		return Math.abs(eSize - rSize + 0.0)/rSize;
	}
	


	/**
	 * F1 score
	 * @param tMap
	 * @param eMap
	 * @return
	 */
	double F1Score(Map<Key, Double> tMap, Map<Key, Double> eMap) {
		// TODO Auto-generated method stub
		double intersectSize = Sets.intersection(tMap.keySet(), eMap.keySet()).size();
		double eSize = eMap.size();
		double tSize = tMap.size();
		
		double precision = intersectSize/eSize ;
		double recall = intersectSize/tSize;
		
		double F1Score = 2 * precision * recall/(precision + recall);
				
		return   F1Score;
	}
	
	/*
	 * main
	 */
	public static void main(String[] args){
		
		QueryLSSFingerFlowApps  tester = new QueryLSSFingerFlowApps("QueryLSSFingerFlowApps");
		try {
			
			//default: n=10000, cluster center = 8, bucket counter = n*.05, fp =0.001, args = 0.val 
			//10000 8 500 0.001 /Users/quanyongf/git/ElasticSketchCode/data/0.datValues 16 2 1000 6
			//number of flow entries
			int _n = Integer.parseInt(args[0]);
			
		
			
			 BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("TestLSSFingerprintExpValues",
						true));
			
			bufferedWriter.write("Trace started\n");
			
			//String onlineFile = args[5];
			
			String onlineFile="";
			String prefix = "/Users/quanyongf/git/ElasticSketchCode/data/";
			 
//			for(int id=1;id<=10;id++){
//				tester.initTest(args, bufferedWriter);
//				
//				onlineFile=prefix+id + ".datValues";
//				tester.entry(onlineFile, _n,bufferedWriter);
//				tester.clear();
//			}//end
			
			bufferedWriter.flush();
			bufferedWriter.close();
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}



}
