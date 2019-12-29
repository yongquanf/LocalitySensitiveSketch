package util.async;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.text.NumberFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;


public class MathUtil {
	double seed;
	Hashtable<Integer, Integer> test = new Hashtable<Integer, Integer>(20);

	/**
	 * 
	 * @param seed
	 *            for [1,100]
	 */
	public MathUtil(double seed) {
		this.seed = seed;
	}

	
    public static String randString(int numChars) {
        int numBits = (int)(numChars * Math.log(36) / Math.log(2));
        return new java.math.BigInteger(numBits, new java.util.Random()).toString(36);
    }
    
    
	/**
	 * 
	 * @param arrays
	 * @param type
	 *            : min, max
	 * @return
	 */
	public float ExtremeValue(float[] arrays, String type) {
		int dim = arrays.length;
		float start = arrays[dim - 1];
		dim--;
		while (dim > 0) {
			if (type.equalsIgnoreCase("min")) {
				start = Math.min(arrays[dim - 1], start);
			} else if (type.equalsIgnoreCase("max")) {
				start = Math.max(arrays[dim - 1], start);
			}
			dim--;
		}
		return start;
	}

	public int[] intervalCount(Vector<Float> range) {
		/* 0.5 0.6,0.7 0.9 1.0, 2.0. 3.0, 4.0+ */
		int size = range.size();
		int[] interval = new int[8];
		for (int i = 0; i < 8; i++) {
			interval[i] = 0;
		}

		for (float f : range) {
			if (in(f, 0, .5)) {
				interval[0]++;
			} else if (in(f, .5, .6)) {
				interval[1]++;
			} else if (in(f, .6, .7)) {
				interval[2]++;
			} else if (in(f, .7, .9)) {
				interval[3]++;
			} else if (in(f, .9, 1.)) {
				interval[4]++;
			} else if (in(f, 1., 2.)) {
				interval[5]++;
			} else if (in(f, 2., 3.)) {
				interval[6]++;
			} else if (in(f, 3., Float.MAX_VALUE)) {
				interval[7]++;
			}

		}
		return interval;

	}

	/**
	 * 
	 * @param requiredNums
	 * @param start
	 * @param end
	 * @return
	 */
	public int[] getnonRepeatedRandomElements(int requiredNums, int start,
			int end) {
		test.clear();
		// System.err.println("\t required "+requiredNums+" from 0 to "+(end-start+1));
		if (requiredNums > (end - start + 1)) {
			System.err.println("\trequired " + requiredNums + " , but "
					+ (end - start + 1));
			return null;
		}
		int iers = 0, i;
		int[] index = new int[requiredNums];
		while (true) {
			if (iers == requiredNums) {
				break;
			}
			i = (int) uniform(start, end);
			if (test.containsKey(i)) {
				continue;
			}
			index[iers] = i; // Note: maybe repeated, but for large matrix
			// ignored is resonable
			test.put(i, iers);
			// System.out.print(index[iers]+" ");
			iers++;
		}
		return index;
	}

	public int[] getnonRepeatedRandomElementsWithExceptions(int requiredNums,
			int start, int end, Hashtable bad) {
		test.clear();
		if (requiredNums > (end - start + 1)) {
			return null;
		}
		int iers = 0;
		int i;
		int[] index = new int[requiredNums];
		while (true) {
			if (iers == requiredNums) {
				break;
			}
			i = (int) uniform(start, end);
			if (test.containsKey(i) || (bad != null && bad.containsKey(i))) {
				continue;
			}
			index[iers] = i; // Note: maybe repeated, but for large matrix
			// ignored is resonable
			test.put(i, iers);
			// System.out.print(index[iers]+" ");
			iers++;
		}
		return index;
	}

	/**
	 * coordinate range for one server
	 * 
	 * @param c
	 * @param radius
	 * @return
	 */
	public float[][] CoordiRange(float[] c, float[] radius, int dim, int index) {
		float[][] range = new float[dim][2];
		System.out.print("==================== ");
		for (int index1 = 0; index1 < dim; index1++) {
			System.out.print("\n$ " + index1 + ": ");
			range[index1][0] = c[index * dim + index1] - radius[index1];
			System.out.print(range[index1][0] + ", ");
			range[index1][1] = c[index * dim + index1] + radius[index1];
			System.out.print(range[index1][1] + "\n");

		}
		return range;

	}

	/**
	 * does a contains b
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	public boolean RangeCntains(float[][] a, float[][] b) {
		int dim = a.length;
		int isContain = 0;
		float[][] intersect = new float[dim][2];
		/**/
		for (int i = 0; i < dim; i++) {
			// a<b
			if (a[i][0] < b[i][0] && a[i][1] > b[i][1]) {
				// alas
				isContain++;
			}
		}
		if (isContain == dim) {
			return true;
		}
		return false;

	}

	public float[][] getMaxRange(float[][] a, float[][] b) {
		if (a == null) {
			return b;
		}

		if (b == null) {
			return a;
		}
		int dim = a.length;
		boolean alas = true;
		float[][] intersect = new float[dim][2];
		/**/
		for (int i = 0; i < dim; i++) {
			if (a[i][0] <= b[i][0]) {
				intersect[i][0] = a[i][0];

			} else {
				intersect[i][0] = b[i][0];

			}
			if (a[i][1] <= b[i][1]) {
				intersect[i][1] = b[i][1];
			} else {
				intersect[i][1] = a[i][1];
			}

		}
		return intersect;

	}

	/**
	 * whether two range intersects, and get the intersections,if not return
	 * null
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	public float[][] testAndSetIntersection(float[][] a, float[][] b) {
		if (a == null) {
			return b;
		}

		if (b == null) {
			return a;
		}
		int dim = a.length;
		boolean alas = true;
		float[][] intersect = new float[dim][2];
		/**/
		for (int i = 0; i < dim; i++) {
			// a<b
			if (a[i][0] > b[i][1] || a[i][1] < b[i][0]) {
				// alas
				alas = false;
				break;
			}
			if (a[i][0] <= b[i][0]) {
				intersect[i][0] = b[i][0];

			} else {
				intersect[i][0] = a[i][0];

			}
			if (a[i][1] <= b[i][1]) {
				intersect[i][1] = a[i][1];
			} else {
				intersect[i][1] = b[i][1];
			}

		}
		if (alas) {
			return intersect;
		} else {
			return null;
		}
	}

	/**
	 * test in [from,to]
	 * 
	 * @param test
	 * @param from
	 * @param to
	 * @return
	 */

	public boolean in(double test, double from, double to) {
		boolean IN = false;
		if (test <= to && test >= from) {
			IN = true;
		}
		return IN;
	}

	public boolean in(float[] myPos, float[][] range, float error) {
		int dim = range.length;
		int IN = 0;
		for (int i = 0; i < dim; i++) {
			if (in(myPos[i], range[i][0], range[i][1])) {
				IN++;
			}
			if (myPos[i] < range[i][0]) {
				if ((-myPos[i] + range[i][0]) / (-range[i][0] + range[i][1]) < error) {
					IN++;
				}
			} else if (myPos[i] > range[i][1]) {
				if ((myPos[i] - range[i][1]) / (-range[i][0] + range[i][1]) < error) {
					IN++;
				}
			}

		}
		if (IN == dim) {
			return true;
		}
		return false;
	}

	/**
	 * 
	 * @param a
	 * @param b
	 * @param norm
	 * @return
	 */
	public static float linear_distance(float[] a, int axystart, float[] b,
			int bxystart, int dims, int norm) {

		float distance = 0.0f;
		boolean notInitialized = false;
		int count = 0;
		for (int i = 0; i < dims; i++) {
			// System.out.print(a[axystart+i]+"-");
			// System.out.print(b[bxystart+i]+"; ");
			if (a[axystart + i] == b[bxystart + i]) {
				count++;
			}
		}
		// System.out.println("\n");
		if (count == dims) {
			notInitialized = true;
		}
		for (int i = 0; i < dims; i++) {
			if (norm == 1) {
				distance += Math.abs(a[axystart + i] - b[bxystart + i]);
			} else {
				distance += Math.pow(Math
						.abs(a[axystart + i] - b[bxystart + i]), norm);
			}
		}
		if (norm == 1) {
			return distance;
		} else {
			return (float) Math.pow(distance, (float) 1.0 / norm);
		}
	}

	public static double linear_distance(double[] a, int axystart, double[] b,
			int bxystart, int dims, int norm) {

		float distance = 0.0f;
		for (int i = 0; i < dims; i++) {
			if (norm == 1) {
				distance += Math.abs(a[axystart + i] - b[bxystart + i]);
			} else {
				distance += Math.pow(Math
						.abs(a[axystart + i] - b[bxystart + i]), norm);
			}
		}
		if (norm == 1) {
			return distance;
		} else {
			return (float) Math.pow(distance, (float) 1.0 / norm);
		}
	}

	/**
	 * uniform distribution [a,b]
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	public double uniform(double a, double b) {
		return a + lcgrand((int) (Math.rint(seed))) * (b - a);
	}

	/**
	 * 
	 * @param count
	 * @param a
	 * @param b
	 * @return
	 */
	public Vector<Integer> unifRands(int count, int a, int b) {
		double uniformGenerator;
		if (count < 0) {
			return null;
		} else {
			Vector<Integer> vi = new Vector<Integer>(count);
			for (int i = 0; i < count; i++) {
				uniformGenerator = uniform(a, b);
				vi.add(i, (int) uniformGenerator);
			}
			return vi;
		}
	}

	/**
	 * 
	 * @param v
	 * @return
	 */
	public static double mean(List v) {
		double sum = 0;
		if (v.size() == 0) {
			return Double.MAX_VALUE;
		} else {
			Iterator iter = v.iterator();
			while (iter.hasNext()) {
				sum += ((Double) iter.next());
			}
			return sum / v.size();
		}

	}
	
	/**
	 * abs error
	 * @param v1
	 * @param v2
	 * @return
	 */
	public static double absoluteError(double v1,double v2){
		return Math.abs(v1-v2);
	}

	/**
	 * |v1-v2|/(v1)
	 * @param v1
	 * @param v2
	 * @return
	 */
	public static double relativeError(double v1,double v2){
		return Math.abs(v1-v2)/Math.abs(v1);
	}
	
	public double min(List vec){
		
		double minV=Double.MAX_VALUE;
		Iterator ier = vec.iterator();
		while(ier.hasNext()){
			double vv=(double)ier.next();
			if(minV>vv){
				minV = vv;
			}
		}
		
		return minV;
	}
	
	/**
	 * mean for specified index in 2d matrix
	 * 
	 * @param v
	 * @param index
	 *            node index
	 * @return
	 */
	public float mean(float[][] v, int index) {
		float sum = 0;
		if (v == null) {
			return Float.MAX_VALUE;
		} else {
			if (index < v.length && index > -1) {
				for (int i = 0; i < v[index].length; i++) {
					if (i != index) {
						sum += v[index][i];
					}
				}
			}
			return sum / v.length;
		}

	}

	public static void sort(List<Float> in, String outfile) {
		Collections.sort(in);
		try {
			BufferedWriter fr = new BufferedWriter(new FileWriter(outfile));

			for (float i : in) {
				fr.write(i + "\n");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param v
	 * @param mean
	 * @return
	 */
	public static double stddev(List v, double mean) {
		double sum = 0.;
		double k;
		if (v.size() <= 1)
			return 0.;
		for (int i = 0; i < v.size(); i++) {
			k = (Double) v.get(i);
			sum += Math.pow(k - mean, 2);
		}
		return Math.sqrt(sum / (v.size() - 1));
	}

	/**
	 * stddev for 2d matrix
	 * 
	 * @param v
	 * @param mean
	 * @return
	 */
	public static double stddev(Float[][] v, double mean) {
		double sum = 0.;
		float k;
		if (v == null) {
			return Double.MIN_VALUE;
		}
		for (int i = 0; i < v.length; i++) {
			for (int j = 0; j < v[i].length; j++) {
				if (i == j) {
					continue;
				}
				k = v[i][j];
				sum += Math.pow(k - mean, 2);
			}
		}
		return Math.sqrt(sum / v.length * v[0].length);
	}

	
	/**
	 * variance
	 * @param v
	 * @param mean
	 * @return
	 */
/*	public static double stddev(List v, double mean) {
		double sum = 0.;
		double k;
		if (v == null) {
			return Double.MIN_VALUE;
		}
		for (int i = 0; i < v.size(); i++) {

				k = (Double)v.get(i);
				sum += Math.pow(k - mean, 2);
		}
		return Math.sqrt(sum / v.size());
	}*/
	
	/**
	 * 
	 * @param v
	 * @param percentile
	 * @return
	 */
	public static double percentile(List v, double percentile) {
		int index = -1;
		if (v.size() == 0) {
			return Double.MAX_VALUE;
		}
		if (v.size() == 1) {
			return  (Double)v.get(0);
		}
		Collections.sort(v);
		// System.out.println(v);
		if (percentile >= 0 && percentile <= 1) {
			index = (int) (Math.rint(percentile * v.size()));
			return (Double)v.get(index);
		} else {
			return Double.MAX_VALUE;
		}
	}

	/**
	 * percentile
	 * @param v
	 * @param percentiles
	 * @return
	 */
	public static double[] percentile(List v, double[] percentiles) {
		int index = -1;
		if (v.size() == 0) {
			return null;
		}
		if (v.size() == 1) {
			return  null;
		}
		Collections.sort(v);
		// System.out.println(v);
		double[] out=new double[percentiles.length];
		
		double percentile;
		for(int i=0;i<percentiles.length;i++){
		
			percentile=percentiles[i];
			
		if (percentile >= 0 && percentile <= 1) {
			index = (int) (Math.rint(percentile * v.size()));
			out[i]= (Double)v.get(index);
			}else{
				System.err.println("percentile input should be <=1");
			}
		}
		return out;
	}
	
	
	/**
	 * cumpute the relative rank loss ,3-5
	 * 
	 * @param latency
	 * @param CoordiLatency
	 * @param dim
	 * @param node
	 * @return
	 */
	public float[] RelativeRankLoss(String latency, String CoordiLatency,
			int dim, int node) {
		/*
		 * read latency file and coordinate file use coordinate file to get
		 * nodes' coordinate distance sort each node according to its relative
		 * distance to other nodes compute the relative rank diff two rankes
		 */
		PrintStream ps = null;
		try {
			ps = new PrintStream(new File("RRL.log"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		// *read latency file and coordinate file
		int dims, nodes;
		BufferedReader LatReader = null;
		BufferedReader CorReader = null;
		float[][] Lat;
		float[][] CoordLat;
		try {
			LatReader = new BufferedReader(new FileReader(new File(latency)));
			CorReader = new BufferedReader(new FileReader(new File(
					CoordiLatency)));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			ps.println("ERROR 1");
			e.printStackTrace();
		}
		if (LatReader == null || CorReader == null) {
			ps.println("ERROR 2!!");
		}
		dims = dim;
		nodes = node;
		Lat = new float[nodes][nodes];
		CoordLat = new float[nodes][nodes];
		float[][] Coords = new float[nodes][dims];
		int index = 0;
		String lat = null;
		String coord = null; // temp
		String[] lats = null;
		String[] coords = null;
		while (true) {
			// read a line
			try {
				lat = LatReader.readLine();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (lat == null) {
				ps.print(index + " \n");
				break;
			}
			//
			// for latency
			// System.out.println("$ line"+index+":coord"+coord);
			lats = lat.split("[   \t]"); // seperated by " "//XMatrix;
			// System.out.println("$ lats: 3?="+lats.length);
			int from, to;
			from = Integer.valueOf(lats[1]);
			to = Integer.valueOf(lats[0]);
			Lat[from][to] = Lat[to][from] = Float.parseFloat(lats[2]);
			// ps.print(Lat[from][to]+" ");
			// ps.print("\n");

			// nextline
			index++;
		}

		index = 0;
		int start = 0, end;
		while (true) {

			try {
				coord = CorReader.readLine();
			} catch (IOException e) {
			}
			if (coord == null) {
				ps.print(index + " \n");
				break;
			}

			coords = coord.split("[   \t]");
			end = coords.length - 1;
			System.out.println("2 ?= " + coords.length);
			/*
			 * while(coords[end].isEmpty()){ end--; }
			 * while(coords[start].isEmpty()){ start++; }
			 */
			// System.out.println("$ "+ coords[start]);
			int k = 0;
			for (int i = 0; i <= end; i++) {
				if (coords[i].isEmpty()) {
					continue;
				}
				Coords[index][k++] = Float.parseFloat(coords[i]);
				// ps.print(coords[i]+" ");
			}
			ps.print("\n");
			// nextline
			index++;
		}

		// System.exit(1);
		// * use coordinate file to get nodes' coordinate distance
		float dist = 0;
		for (int i = 0; i < nodes; i++) {
			CoordLat[i][i] = 0;
			for (int j = i + 1; j < nodes; j++) {
				CoordLat[i][j] = CoordLat[j][i] = MathUtil.linear_distance(
						Coords[i], 0, Coords[j], 0, dim, 2);
				// System.out.print(CoordLat[i][j]+" ");
			}
			// System.out.println();
		}
		// * sort each node according to its relative distance to other nodes
		int size = node;
		int[] rands = null;
		float dis1, dis2;
		float lat1, lat2;
		index = 0;
		float[] rrls = new float[size];
		while (index < size) {
			float rrl = 0;
			for (int i = 0; i < size; i++) {
				rands = getnonRepeatedRandomElements(2, 0, size);
				if (rands[0] == index || rands[1] == index) {
					continue;
				}
				dis1 = CoordLat[index][rands[0]];
				dis2 = CoordLat[index][rands[1]];
				lat1 = Lat[index][rands[0]];

				lat2 = Lat[index][rands[1]];
				if ((dis1 < dis2 && (lat1 < lat2))
						|| ((dis1 > dis2) && (lat1 > lat2))) {
					// correct
					continue;
				} else {
					rrl += 1;
				}

			}
			// -------------------------------------------------
			rrl = (rrl) / (size);
			rrls[index] = rrl;
			// -------------------------------------------------
			ps.println(rrls[index]);
			// System.out.println("$ rrl: "+index+", "+rrls[index]);
			index++;

		}

		try {
			LatReader.close();
			CorReader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//
		ps.close();
		return rrls;
	}
	
	

	/**
	 * transformed to double array
	 * @param items
	 * @return
	 */
	public double[] toArray(Collection<Double> items){
		
	 /*   Double[] t=(Double[])items.toArray(new Double[1]);
	    double[]t1=new double[t.length];
	    for(int i=0;i<t.length;i++){
	    	t1[i]=t[i].doubleValue();
	    }
	    t=null;
	    return t1;*/
		
		
		int size=items.size();
		if(size>0){
			double[]t1=new double[size];
			int index=0;
			Iterator<Double> ier = items.iterator();
			while(ier.hasNext()){
				Double dd = ier.next();
				if(dd.isNaN()||dd.isInfinite()){
					t1[index]=0;
					
				}else{
					t1[index]=dd.doubleValue();
				}				
				index++;
			}
			
			return t1;
		}else{
			return null;
		}
		
		
	}
	
	void calculateCDF(double[] dat, BufferedWriter TestCaseStream) throws IOException{
		
		/* PrintWriter TestCaseStream = new PrintWriter(new BufferedOutputStream(new FileOutputStream(new File(
				 name + ".log"),true)));*/
		 
		StatCDF_Histogram stat=new StatCDF_Histogram(dat);	  
		//100 cdf, 10 buckets
		//20 cdf
		stat.calculate(50,10);
		//TestCaseStream.append(datSet +"\n");   
		//TestCaseStream.append("\nCDF: ");
		TestCaseStream.append(toString(stat.getCDF())+"\n");
		
/*		TestCaseStream.append("\nHistgram: ");
		TestCaseStream.append(toString(stat.getHistogram())+"\n");*/
		
		TestCaseStream.flush();
		
	}
	
	  public static String toString(double[] vector) { return toString(vector, " "); }
	  
	  public static String toString(double[] vector, String sep) {
			StringBuffer sb = new StringBuffer();
//			sb.append("(");
			if (vector != null) {
			  for (int i = 0; i < vector.length; i++) {
				sb.append(fpFormat.format(vector[i]));
				if (i < (vector.length - 1))
				  sb.append(sep);
			  }
			} else{
			  //sb.append("null");
			}
//			sb.append(")");
			return sb.toString();
		  }
	  
	  private static NumberFormat fpFormat = NumberFormat.getNumberInstance();
	  private static NumberFormat intFormat = NumberFormat.getIntegerInstance();

	  static {
		fpFormat.setMaximumFractionDigits(2);
		fpFormat.setMinimumFractionDigits(2);
		fpFormat.setGroupingUsed(false);    
		
		intFormat.setGroupingUsed(false);
		intFormat.setMinimumFractionDigits(0);
		intFormat.setMaximumFractionDigits(0);
	  }	

	public float RelativeRankLoss(float[] latency, float[] CoordiLatency) {
		return 0.0f;
	}

	/*
	 * generate the random number with uniform ditribution
	 */
	private long MODLUS = 2147483647;
	private long MULT1 = 24112;
	private long MULT2 = 26143;

	/* Set the default seeds for all 100 streams. */

	static long zrng[] = { 1, 1973272912, 281629770, 20006270, 1280689831,
			2096730329, 1933576050, 913566091, 246780520, 1363774876,
			604901985, 1511192140, 1259851944, 824064364, 150493284, 242708531,
			75253171, 1964472944, 1202299975, 233217322, 1911216000, 726370533,
			403498145, 993232223, 1103205531, 762430696, 1922803170,
			1385516923, 76271663, 413682397, 726466604, 336157058, 1432650381,
			1120463904, 595778810, 877722890, 1046574445, 68911991, 2088367019,
			748545416, 622401386, 2122378830, 640690903, 1774806513,
			2132545692, 2079249579, 78130110, 852776735, 1187867272,
			1351423507, 1645973084, 1997049139, 922510944, 2045512870,
			898585771, 243649545, 1004818771, 773686062, 403188473, 372279877,
			1901633463, 498067494, 2087759558, 493157915, 597104727,
			1530940798, 1814496276, 536444882, 1663153658, 855503735, 67784357,
			1432404475, 619691088, 119025595, 880802310, 176192644, 1116780070,
			277854671, 1366580350, 1142483975, 2026948561, 1053920743,
			786262391, 1792203830, 1494667770, 1923011392, 1433700034,
			1244184613, 1147297105, 539712780, 1545929719, 190641742,
			1645390429, 264907697, 620389253, 1502074852, 927711160, 364849192,
			2049576050, 638580085, 547070247 };

	/* Generate the next random number. */

	public double lcgrand(int stream) {
		long zi, lowprd, hi31;

		zi = zrng[stream];
		lowprd = (zi & 65535) * MULT1;
		hi31 = (zi >> 16) * MULT1 + (lowprd >> 16);
		zi = ((lowprd & 65535) - MODLUS) + ((hi31 & 32767) << 16)
				+ (hi31 >> 15);
		if (zi < 0)
			zi += MODLUS;
		lowprd = (zi & 65535) * MULT2;
		hi31 = (zi >> 16) * MULT2 + (lowprd >> 16);
		zi = ((lowprd & 65535) - MODLUS) + ((hi31 & 32767) << 16)
				+ (hi31 >> 15);
		if (zi < 0)
			zi += MODLUS;
		zrng[stream] = zi;
		return ((zi >> 7 | 1) / 16777216.0);
	}

	public void lcgrandst(long zset, int stream) /*
												 * Set the current zrng for
												 * stream "stream" to zset.
												 */
	{
		zrng[stream] = zset;
	}

	public long lcgrandgt(int stream) /*
									 * Return the current zrng for stream
									 * "stream".
									 */
	{
		return zrng[stream];
	}

	public static void main(String[] args) {
		MathUtil math = new MathUtil(100);
		// math.RelativeRankLoss(args[0], args[1], Integer.parseInt(args[2]),
		// Integer.parseInt(args[3]));
	}


	public double percentIsZero(List<Double> errors) {
		// TODO Auto-generated method stub
		if(errors==null||errors.isEmpty()){
			return 0;
		}
		Iterator<Double> ier = errors.iterator();
		int total=0;
		int size=errors.size();
		while(ier.hasNext()){
			if(ier.next()!=0){
				total++;
			}
		}
		return (size-total + 0.0)/size;
		
	}
}
