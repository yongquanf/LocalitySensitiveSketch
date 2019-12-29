package util.async;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import edu.harvard.syrah.prp.POut;
import edu.harvard.syrah.prp.Stat;
import java.util.Arrays;

/**
 * 
 * This class gathers various statistics.
 *
 */
public class StatCDF_Histogram {
	//private static final Log log = new Log(Stat.class);
	
	private static final int DEFAULT_CDF_BUCKET_NUM = 100;
	private static final double DEFAULT_BUCKET_SIZE = 1.0;
	
	public double[] data;
	private double[][] dataXY;
	private double[] sortedData;
	private double[] distribution;
	
	private double[][] histogram;
	
	private int index = 0;
	
	private boolean dataExists = false;
	
	private double max = Double.NaN;
  private double min = Double.NaN;
	private double sum = Double.NaN;
	private double avg = Double.NaN;
	private double stdVal = Double.NaN;
	private double CIValue = Double.NaN;
	private final double stdCI = 1.96;
	
	public StatCDF_Histogram(double[] data) {
		this.data = data;
		if (data.length > 0)
			this.dataExists = true;
	}
	
	public StatCDF_Histogram(int[] data) {
		this.data = new double[data.length];
		for (int i = 0; i < data.length; i++)
			this.data[i] = data[i];
		if (data.length > 0)
			this.dataExists = true;
	}
	
	public StatCDF_Histogram(int dataSize) {
		this.data = new double[dataSize];
		this.dataXY = new double[dataSize][2];
	}
		
	public StatCDF_Histogram(ArrayList<Double> pValues4BT1) {
		// TODO Auto-generated constructor stub
		int size = pValues4BT1.size();
		this.data=new double[size];
		Iterator<Double> ier = pValues4BT1.iterator();
		int ind = 0;
		while(ier.hasNext()){
			data[ind] = ier.next();
			ind++;
		}
		pValues4BT1.clear();
		pValues4BT1=null;
		
		if (data.length > 0)
			this.dataExists = true;
		
	}

	public void addData(double dataItem) {
		data[index] = dataItem;
		this.dataExists = true;
    index++;
	}
	
	public void addData(double x, double y) {
		this.dataXY[index][0] = x;
		this.dataXY[index++][1] = y;		
		this.dataExists = true;
	}
	
	public void divideBy(double d) {
		for (int i = 0; i < data.length; i++)
			data[i] = data[i] / d;
	}
	
	public void calculate() {
		calculate(DEFAULT_CDF_BUCKET_NUM, DEFAULT_BUCKET_SIZE);
	}
	
	public void calculate(double bucketSize) {
		calculate(DEFAULT_CDF_BUCKET_NUM, bucketSize);
	}
	
	/**
	 * confidence interval for the input samples
	 * @return
	 */
	public void computeConfidenceInterval95(){
		CIValue = stdCI*(0.0+stdVal)/Math.sqrt(sortedData.length);
		
	}
	
	public void stddev() {
		double sum = 0.;
		double k;
		//double mean=.avg;
		
		for (int i = 0; i < sortedData.length; i++) {
			k = sortedData[i];
			sum += Math.pow(k - avg, 2);
		}
		stdVal = Math.sqrt(sum / (sortedData.length - 1));
	}
	
	/**
	 * get zero percentage
	 * @return
	 */
	public double getZeroPercentage(){
		double totalNum=0;
		for(int i=0;i<data.length;i++){
			if(data[i]>0){
				totalNum++;
			}
		}
		return 1-totalNum/(0.0+data.length);
	}
	
	/**
	 * specify the number of buckets
	 * @param cdfBucketNum
	 * @param bucketNum
	 */
	public void calculate(int cdfBucketNum, int bucketNum) {
		if (!dataExists)
			return;

		// Calculate the distribution function
		calculateCDF(cdfBucketNum);
		
		// Calculate the maximum value
		max = sortedData[data.length - 1];
    min = sortedData[0];
		
		// Calculate the histogram
        double bucketSize=max/bucketNum;
		//int bucketNum = (int) Math.ceil(max / bucketSize);
		histogram = new double[bucketNum + 1][2];
		int index = 0;
		for (int i = 0; i < bucketNum; i++) {
			histogram[i + 1][0] = (i + 1) * bucketSize;
			while (index < sortedData.length && sortedData[index] <= ((i + 1) * bucketSize)) {
				histogram[i + 1][1]++;
				index++;
			}
		}
		
		sum = 0;
		for (int i = 0; i < data.length; i++)
			sum += data[i];
		
		avg = sum / data.length;		
		
		//std
				stddev();
				computeConfidenceInterval95();
	}
	
	public void calculate(int cdfBucketNum, double bucketSize) {
		if (!dataExists)
			return;

		// Calculate the distribution function
		calculateCDF(cdfBucketNum);
		
		// Calculate the maximum value
		max = sortedData[data.length - 1];
    min = sortedData[0];
		
		// Calculate the histogram
		int bucketNum = (int) Math.ceil(max / bucketSize);
		histogram = new double[bucketNum + 1][2];
		int index = 0;
		for (int i = 0; i < bucketNum; i++) {
			histogram[i + 1][0] = (i + 1) * bucketSize;
			while (index < sortedData.length && sortedData[index] <= ((i + 1) * bucketSize)) {
				histogram[i + 1][1]++;
				index++;
			}
		}
		
		sum = 0;
		for (int i = 0; i < data.length; i++)
			sum += data[i];
		
		avg = sum / data.length;		
		
		//std
		stddev();
		computeConfidenceInterval95();
	}
	
	private void calculateCDF(int numBuckets) {		
		distribution = new double[numBuckets];		
		sortedData = new double[data.length];
		System.arraycopy(data, 0, sortedData, 0, data.length);
		Arrays.sort(sortedData);
		
		for (int i = 0; i < numBuckets; i++) {			
			double bucketIndex = (double) sortedData.length * (i + 1) / numBuckets;
			int arrayIndex = ((int) Math.ceil(bucketIndex)) - 1;
			if(arrayIndex<0){
				arrayIndex=0;
			}
			//log.debug("arrayIndex=" + arrayIndex);
		/*	if (i < distribution.length / data.length)
				distribution[i] = Double.NaN;
			else*/
				distribution[i] = sortedData[arrayIndex];
		}		
	}
	
	public double[] getData() {
		if (!dataExists)
			return null;
		double[] dataArray = new double[index];
		System.arraycopy(data, 0, dataArray, 0, index);
		return dataArray;
	}
	
	public double[][] getDataXY() {
		return dataExists ? dataXY : null;
	}
	
	public double[] getCDF() {
		return dataExists ? distribution : null;
	}
	
	public double getCDFValue(int bucket) {
		return dataExists ? distribution[bucket - 1] : Double.NaN;
	}
	
	public double[][] getHistogram() {
		return dataExists ? histogram : null;
	}
	
	public double getMax() {
		return dataExists ? max : Double.NaN;
	}
  
  public double getMin() {
    return dataExists ? min : Double.NaN;
  }
	
	public double getSum() {
		return dataExists ? sum : Double.NaN;
	}
	
	public double getAverage() {
		return dataExists ? avg : Double.NaN;
	}
	
	public static void main(String[] argv) {
		
		double[] data = new double[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
		//double[] data = new double[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
		//double[] data = new double[] {-2, 7, 7, 4, 18, -5};
		//double[] data = new double[1000];
		//double[] data2 = new double[1000];
		
		for (int i = 0; i < data.length; i++) {
			//data[i] = Util.getRandom().nextGaussian();
			//data[i] = Util.getRandomPosInt(1000);
			//data2[i] = Util.getRandom().nextDouble();
		}
		
		POut.p("data=" + POut.toString(data));
		
		Stat stat = new Stat(data);
		stat.calculate(100, 1.0);
		
		POut.p("max=" + stat.getMax());
		double[] distribution = stat.getCDF();		
		POut.p("distribution=" + POut.toString(distribution));
		double[][] histogram = stat.getHistogram();		
		POut.p("histogram=" + POut.toString(histogram));
		POut.p("50th percentile=" + stat.getCDFValue(50));
		
		/*
		Stat stat2 = new Stat(data2);
		stat2.calculateCDF(1000);
		distribution = stat2.getCDF();
		Util.p("distribution2=" + Util.toString(distribution));
		*/		
	}

	public Object getCI() {
		// TODO Auto-generated method stub
		return CIValue;
	}
}