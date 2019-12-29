package util.async;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Vector;

import edu.harvard.syrah.prp.PUtil;

public class Matrix {
	public class record {
		public String sp; // start
		public String ep; // end
		public float latency;// latency
	}

	public Vector<HashMap<Integer, record>> latencyList; // load the latency
	// matrix into a
	// vector of vector
	// for each point,
	// sorted by rows
	public record cuRecord;
	public static Vector<record> recordList;
	public float[][] latencyMatrix;
	public float[][] CoordMatrix;
	public Vector<InetAddress> nodes;
	public int rows;// -1
	public int colums;
	public int format;
	public int line;
	public int[] badNodes; // 'bad' landmarks
	public int starts0;

	
	public float minimum;
	public float maximum;
	public float average;
	
	/*
	 * format: 1: a b a_bLatency default 2: row, colum, latency matrix 3: row
	 * ,colum, address latency ...
	 */
	public Matrix() {
		latencyList = new Vector<HashMap<Integer, record>>();
		cuRecord = null;
		recordList = new Vector<record>(1);
		latencyMatrix = null;
		CoordMatrix = null;
		rows = colums = Integer.MIN_VALUE;
		format = 1;
		line = -1;
		nodes = new Vector<InetAddress>(1);
		starts0 = 0;

		
		minimum=Float.MAX_VALUE;
		maximum=Float.MIN_VALUE;
		average=0;
		
	}

	
	void updateSta(float val){
		if(val<minimum){
			minimum=val;
		}
		if(val>maximum){
			maximum=val;
		}
		
	}
	
	/*
	 * read a truelatency matrix, format: a b lantency
	 */
	public void readlatencyMatrix(String truelatency) {
		int temp0, temp1;
		line = -1;

		if (truelatency != null) {
			try {
				BufferedReader rf= new BufferedReader(new FileReader(new File(truelatency)));
				//RandomAccessFile rf = new RandomAccessFile(truelatency, "r");
				if (rf == null) {
					System.err.println("ERROR, empty file  !!");
					return;
				} else {
					String cuLine;

					while (true) {

						cuLine = rf.readLine();
						line++; // current line
						// System.out.println("@: CurLine "+cuLine);
						if (cuLine == null) {
							System.out.println("@: Panic " + line);
							break;
						}
						if (cuLine.startsWith("#")) {
							continue;
						}
						//trim the empty header
						cuLine=trimEmpty(cuLine);
						// write cuLine into record list
						// System.out.println("@: CurLine "+cuLine);
						String[] s = cuLine.split("[ \\s\t ]"); // seperated by
						// " "//XMatrix
						// System.out.format("%s",cuLine);
						// System.out.println();
						// ===================================================
						if (getFormat() == 1) {
							if (line == 0) {
								/* row */
								rows = Integer.parseInt(s[0]);
							} else if (line == 1) {
								colums = Integer.parseInt(s[0]);
								System.out.format("$ array is%d %d\n", rows,
										colums);
								initMatrix();

							} else {
								// System.out.println("% "+s.length+" ");

								// ======================================
								int a = Integer.parseInt(s[0]);
								int b = Integer.parseInt(s[1]);

								// TODO:start from 1
								float v = Float.parseFloat(s[2]);

								latencyMatrix[a - this.starts0][b
										- this.starts0] = v;
								latencyMatrix[b - this.starts0][a
										- this.starts0] = v;
								// System.out.println("% "+v+
								// " "+latencyMatrix[a-this.starts0][b-this.starts0]+" ,"+latencyMatrix[b-this.starts0][a-this.starts0]);
								// badNodes[a]--;
								updateSta(v);
								
							}

							// System.out.format("$ is%d %d %d %d \n",temp0,temp1,rows,colums);

						} else if (format == 2) {
							// TODO: error when matlab save data
							/* latency matrix */
							if (line == 0) {
								/* row */
								this.rows = Integer.parseInt(s[0]);
								System.out.println("$ rows: " + this.rows);
							} else if (line == 1) {
								this.colums = Integer.parseInt(s[0]);
								System.out.format("$ array is%d %d\n", rows,
										colums);
								initMatrix();
								// MinMatrix();
								// initbadNodes(colums);
							} else {
								// System.out.println("% "+s.length+" ");
								int start = 0;
								int end = s.length;
								// TODO seperate by pattern
								// System.out.println("% start"+start+"  end "+end);
								if (s[start].isEmpty()) {
									start++;
								}
								for (int i = 0; start < end;) {
									// latencyMatrix[line-2][i]=Float.parseFloat(s[i]);
									if (s[start].isEmpty()) {
										// nothing
									} else {
										latencyMatrix[line - 2][i] = Float.parseFloat(s[start].trim());
										updateSta(latencyMatrix[line - 2][i]);
										i++;
									}
									
									// System.out.format(" <"+(line-2)+","+i+"> %f ",latencyMatrix[line-2][i]);
									start++;
								}
								// System.out.println();
							}
						}

						else if (getFormat() == 3) {
							/* address matrix */
							if (line == 0) {
								/* row */
								this.rows = Integer.parseInt(s[0]);
							} else if (line == 1) {
								this.colums = Integer.parseInt(s[0]);
								initMatrix();
							} else {
								nodes.add(InetAddress.getByName(s[0]));
								for (int i = 0; i < colums; i++) {
									latencyMatrix[line][i] = Float
											.parseFloat(s[i + 1]);
									
									updateSta(latencyMatrix[line][i]);
								}

							}
						}

					}

				}
				System.out.println("@: Panic " + line);
				rf.close();
				PUtil.gc();
				return;

			} catch (Exception e) {
				e.printStackTrace();
			}
			
			
		}else{
			
			System.err.println(" empty file");
			return;
		}
	}

	/**
	 * with -1
	 * 
	 */
	public void initMatrix() {
		latencyMatrix = new float[rows][colums];
		for (int i = 0; i < rows; i++) {
			for (int j = 0; j < rows; j++)
				latencyMatrix[i][j] = 0;
		}

	}

	public boolean writeLatencyMatrix(String truelatency) {
		this.readlatencyMatrix(truelatency);
		try {
			BufferedWriter b = getWriter(new File(truelatency + ".out"));
			int count = 0;
			for (int i = 0; i < rows; i++) {
				for (int j = 0; j < colums; j++) {
					if (i != j && latencyMatrix[i][j] == -1) {
						System.out.println(" " + i + " " + j);
						count++;
						latencyMatrix[i][j] = 0;
					} else if (i == j) {
						latencyMatrix[i][j] = 0;
					}
					b.write(latencyMatrix[i][j] + " ");
				}

				b.newLine();
			}
			System.out.println("\n================\n" + count);
			b.flush();
			b.close();
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;

	}

	public BufferedWriter getWriter(File aFile) throws IOException {
		return new BufferedWriter(new FileWriter(aFile));
	}

	public boolean ReadCoordinate(File truelatency) {
		int temp0, temp1;
		line = -1;
		if (truelatency != null) {
			try {
				RandomAccessFile rf = new RandomAccessFile(truelatency, "r");
				if (rf == null) {
					return false;
				} else {
					String cuLine;

					while (true) {

						cuLine = rf.readLine();
						line++; // current line
						System.out.println("@: CurLine " + cuLine);
						if (cuLine == null) {
							System.out.println("@: Panic " + line);
							break;
						}
						if (cuLine.startsWith("#")) {
							continue;
						}
						// write cuLine into record list
						// System.out.println("@: CurLine "+cuLine);
						if (line == 0) {
							/* row */
							rows = Integer.parseInt(cuLine.trim());
						} else if (line == 1) {
							colums = Integer.parseInt(cuLine.trim());
							System.out
									.format("$ array is%d %d\n", rows, colums);
							CoordMatrix = new float[rows][colums];
							// MinMatrix();
							// initbadNodes(colums);
						} else {
							String[] s = cuLine.split("[   \t]"); // seperated
							// by
							// " "//XMatrix
							// coordinate
							System.out.println("% " + s.length + " ");
							int start = 1;
							int end = s.length - 1;
							// TODO seperate by pattern
							System.out.println("% start" + start + "  end "
									+ end);
							for (int i = start; i < end + 1; i++) {
								System.out.println("% " + i + "  : " + s[i]);
							}
							for (int i = 0; i < this.colums; i++) {
								// latencyMatrix[line-2][i]=Float.parseFloat(s[i]);
								while (s[i + start].trim().isEmpty()
										|| s[i + start].trim() == null) {
									start++;
								}
								CoordMatrix[line - 2][i] = Float.parseFloat(s[i
										+ start].trim());
								System.out.format(" %f ",
										CoordMatrix[line - 2][i]);
							}
						}

					}
				}
			} catch (Exception e) {
				System.out.println(e.toString());
			}
			;

		}
		return true;
	}

	/**
	 * write to this.latencyMatrix
	 * 
	 * @return
	 */
	public boolean writePairwiseDistance() {
		if (this.CoordMatrix != null) {
			int nodes = this.rows;
			int dim = this.colums;
			MathUtil math = new MathUtil(100);
			float dis;
			if (this.latencyMatrix == null) {
				this.latencyMatrix = new float[nodes][nodes];
			} else {
				// nana
			}
			try {
				for (int index = 0; index < nodes; index++) {
					for (int to = index; to < nodes; to++) {
						dis = MathUtil.linear_distance(this.CoordMatrix[index],
								0, this.CoordMatrix[to], 0, dim, 2);
						this.latencyMatrix[index][to] = this.latencyMatrix[to][index] = dis;
					}
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return true;
		}
		return false;
	}

	/**
	 * write pairwise distance matrix i,j dis
	 * 
	 * @return
	 */
	public boolean writePairwiseDistance(String fileName) {
		if (this.CoordMatrix != null) {
			int nodes = this.rows;
			int dim = this.colums;
			MathUtil math = new MathUtil(100);
			float dis;
			try {
				PrintStream ps = new PrintStream(new File(fileName));
				for (int index = 0; index < nodes; index++) {
					for (int to = 0; to < nodes; to++) {
						dis = MathUtil.linear_distance(this.CoordMatrix[index],
								0, this.CoordMatrix[to], 0, dim, 2);
						ps.println(index + " " + to + " " + dis);
					}
				}
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return true;
		}
		return false;
	}

	public void MinMatrix() {
		if (latencyMatrix != null) {
			for (int i = rows; i > 0; i--) {
				for (int j = colums; j > 0; j--) {
					latencyMatrix[i - 1][j - 1] = Float.MIN_VALUE;
				}
			}
		}
	}

	/*
	 * read a icmprecords, latency measurements may miss
	 */
	public boolean readlatencyMeasurements(File icmprecords) {

		return true;
	}

	public void initbadNodes(int colums) {
		badNodes = new int[colums];
		for (int i = 0; i < colums; i++) {
			badNodes[i] = colums;
		}
	}

	/**
	 * list the 'correct' landmark candidates
	 * 
	 * @return 'correct' landmarks
	 */
	public int[] landmarkCandidates() {
		return null;
	}

	/*
	 * if the recordlist not empty, fill the matrix we have to readlatencyMatrix
	 * first. format: a b latency
	 */
	public boolean fillLatencyMatrix() {
		int recordSize = recordList.size();
		if (recordSize > 0) {
			if (getFormat() == 1) {
				rows++;
				colums++;
				latencyMatrix = new float[this.rows + 1][this.colums + 1];
				for (int i = 0; i <= this.rows; i++) {
					for (int j = 0; j <= this.colums; j++) {
						latencyMatrix[i][j] = Float.MIN_VALUE;
					}
				}
				System.out.println("\n$ Matrix nodes: rows" + (this.rows + 1)
						+ "colums" + (this.colums + 1));
				int index = 0;

				while (recordSize > 0) {
					record r = recordList.elementAt(index++);
					// System.out.println("\n$ Matrix nodes:"+r.sp+"\t:"+r.ep);
					latencyMatrix[Integer.parseInt(r.sp)][Integer
							.parseInt(r.ep)] = r.latency;
					// latencyMatrix[r.ep][r.sp]=r.latency; // assume symmetric
					// duplex
					recordSize--;
				}
			}
			return true;
		}

		return false;

	}

	public int getRows() {
		// System.out.println("$: ROWS "+this.rows);
		return this.rows;
	}

	public int getColums() {
		return this.colums;
	}

	/*
	 * get the latency in(row,colum)
	 */
	public float get(int row, int colum) {

		// System.out.println("Now we have row:"+row+" colum:"+colum);
		return (float) latencyMatrix[row][colum];

	}

	public boolean exists(int row, int colum) {
		if (latencyMatrix[row][colum] != Float.MIN_VALUE) {
			return true;
		}
		return false;
	}

	public int getFormat() {
		return format;
	}

	public void setFormat(int format) {
		this.format = format;
	}
	
	public static String trimEmpty(String s){
		String s1=s.trim();
		while(s1.startsWith("[ (\\s\t]")){
			s1=s1.substring(1);
		}
		while(s1.endsWith("[ )\\s\t]")){
			s1=s1.substring(0,s1.length());
		}
		return s1;
	}

	public static void main(String[] args) {
		/*
		 * String []s={"0.0000000e+000"," 1.8600626e+000"}; //float d1,d2;
		 * //System
		 * .out.println("$ :"+(d1=float.parsefloat(s[0]))+", "+(d2=float
		 * .parsefloat(s[1]))+" = "+(d1+d2)); String
		 * s1="Clus560_coordinates.dat"; Matrix mx=new Matrix();
		 * mx.ReadCoordinate(new File(s1));
		 * mx.writePairwiseDistance("outClus560");
		 */
/*		Matrix mx = new Matrix();
		mx.format = 1;
		mx.starts0 = 1;
		mx.writeLatencyMatrix(args[0]);*/
		
		String cuLine=" 1 2 0.6";
		String[] s = cuLine.split("[ \\s\t ]"); // seperated by
		System.out.println(s.length+", "+s[0]);
		cuLine= trimEmpty(cuLine);
		 s = cuLine.split("[ \\s\t ]"); // seperated by
		System.out.println(s.length+", "+s[0]);
		
	}

}
