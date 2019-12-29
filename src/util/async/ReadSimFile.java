package util.async;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class ReadSimFile {

	Map<String, Integer> NodeIndex = new HashMap<String, Integer>(1);
	int nodeCount;
	List<pair> Capacity = new ArrayList<pair>(1);
	List<pair> Bandwidth = new ArrayList<pair>(1);
	List<pair> bottleneck_bd = new ArrayList<pair>(1);

	public class pair {
		int from;
		int to;
		double val;

		public pair(int _from, int _to, double _val) {
			from = _from;
			to = _to;
			val = _val;
		}
	}

	private FileInputStream file;

	public void Read_HPDataSet_BW(String file) throws IOException {

		nodeCount = 0;
		BufferedReader reader = new BufferedReader(new FileReader(file));

		int from, to;

		String[] st;
		String line;
		int index = -1;
		int lineCounter = -1;
		while ((line = reader.readLine()) != null) {
			lineCounter++;
			// System.out.println("$: "+lineCounter);
			// new line
			st = line.split(",");
			int num = st.length;
			if (num != 8) {
				assert (false);
			}
			// node index
			if (!NodeIndex.containsKey(st[0])) {
				index++;
				NodeIndex.put(st[0], Integer.valueOf(index));
				from = index;
			} else {
				from = NodeIndex.get(st[0]).intValue();
			}
			if (!NodeIndex.containsKey(st[1])) {
				index++;
				NodeIndex.put(st[1], Integer.valueOf(index));
				to = index;
			} else {
				to = NodeIndex.get(st[1]).intValue();
			}

			// System.out.println(st[4]+" : "+st[5]+" : "+st[6]);
			// capacity,
			if (!st[4].equalsIgnoreCase("N/A"))
				Capacity.add(new pair(from, to, Double.parseDouble(st[4])));

			// bandwidth,
			if (!st[5].equalsIgnoreCase("N/A"))
				Bandwidth.add(new pair(from, to, Double.parseDouble(st[5])));

			// bottleneck_bd
			if (!st[6].equalsIgnoreCase("N/A"))
				bottleneck_bd
						.add(new pair(from, to, Double.parseDouble(st[6])));

		}

		nodeCount = NodeIndex.keySet().size();
	}

	/**
	 * capacity, bandwidth, bottleneck_bd
	 * 
	 * @param file
	 * @throws IOException
	 */
	public void writeFile(String file) throws IOException {
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(
				file)));

		double[][] val = new double[nodeCount][nodeCount];
		Vector<Integer> forbidden = new Vector<Integer>(1);
		for (int i = 0; i < nodeCount; i++) {

			for (int j = 0; j < nodeCount; j++) {
				if (i == j) {
					val[i][j] = 0;
				} else {
					val[i][j] = -1;
				}

			}
		}
		Iterator<pair> ier;

		if (file.equalsIgnoreCase("capacity")) {

			ier = this.Capacity.iterator();

			while (ier.hasNext()) {
				pair tmp = ier.next();
				val[tmp.from][tmp.to] = tmp.val;
			}

			int empty = 0;
			for (int i = 0; i < nodeCount; i++) {

				for (int j = 0; j < nodeCount; j++) {
					if (val[i][j] == -1) {
						empty++;
					}
				}
				if (empty > 0.5 * nodeCount) {
					forbidden.add(Integer.valueOf(i));
				}
				empty = 0;
			}

			out.println(nodeCount - forbidden.size());

			for (int i = 0; i < nodeCount; i++) {
				if (forbidden.contains(Integer.valueOf(i))) {
					continue;
				}
				for (int j = 0; j < nodeCount; j++) {
					if (forbidden.contains(Integer.valueOf(j))) {
						continue;
					}
					out.write(val[i][j] + " ");
				}
				out.write("\n");
			}

		}
		if (file.equalsIgnoreCase("bandwidth")) {

			ier = this.Bandwidth.iterator();

			while (ier.hasNext()) {
				pair tmp = ier.next();
				val[tmp.from][tmp.to] = tmp.val;
			}

			int empty = 0;
			for (int i = 0; i < nodeCount; i++) {

				for (int j = 0; j < nodeCount; j++) {
					if (val[i][j] == -1) {
						empty++;
					}
				}
				if (empty > 0.5 * nodeCount) {
					forbidden.add(Integer.valueOf(i));
				}
				empty = 0;
			}

			out.println(nodeCount - forbidden.size());

			for (int i = 0; i < nodeCount; i++) {
				if (forbidden.contains(Integer.valueOf(i))) {
					continue;
				}
				for (int j = 0; j < nodeCount; j++) {
					if (forbidden.contains(Integer.valueOf(j))) {
						continue;
					}
					out.write(val[i][j] + " ");
				}
				out.write("\n");
			}

		}

		if (file.equalsIgnoreCase("bottleneck_bd")) {

			ier = this.bottleneck_bd.iterator();

			while (ier.hasNext()) {
				pair tmp = ier.next();
				val[tmp.from][tmp.to] = tmp.val;
			}

			int empty = 0;
			for (int i = 0; i < nodeCount; i++) {

				for (int j = 0; j < nodeCount; j++) {
					if (val[i][j] == -1) {
						empty++;
					}
				}
				if (empty > 0.5 * nodeCount) {
					forbidden.add(Integer.valueOf(i));
				}
				empty = 0;
			}

			out.println(nodeCount - forbidden.size());

			for (int i = 0; i < nodeCount; i++) {
				if (forbidden.contains(Integer.valueOf(i))) {
					continue;
				}
				for (int j = 0; j < nodeCount; j++) {
					if (forbidden.contains(Integer.valueOf(j))) {
						continue;
					}
					out.write(val[i][j] + " ");
				}
				out.write("\n");
			}
		}

		out.flush();
		out.close();

	}

	public static void main(String[] args) {

		ReadSimFile test = new ReadSimFile();
		try {
			test.Read_HPDataSet_BW(args[0]);
			test.writeFile("Capacity");
			test.writeFile("Bandwidth");
			test.writeFile("bottleneck_bd");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
