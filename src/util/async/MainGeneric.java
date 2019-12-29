package util.async;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.prp.NetUtil;
import edu.harvard.syrah.prp.PUtil;
import edu.harvard.syrah.sbon.async.CBResult;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;
import edu.harvard.syrah.sbon.async.comm.AddressFactory;
import edu.harvard.syrah.sbon.async.comm.AddressIF;
import edu.harvard.syrah.sbon.async.comm.NetAddress;

public class MainGeneric {

	static Log log = new Log( MainGeneric.class);
	
	//public static ExecutorService execMain = Executors.newFixedThreadPool(15);
	
	public static ExecutorService execMain = Executors.newFixedThreadPool(5);
	
	public static void createThread(String string, Runnable oem) {
		Thread t = new Thread(oem);
		t.setName(string);
		//t.setDaemon(true);
		t.start();

	}

	public static void printAddress(Set<AddressIF> nodes){
		Iterator<AddressIF> ier = nodes.iterator();
		while(ier.hasNext()){
			NetAddress tmp=(NetAddress)ier.next();
			log.info("$: host: "+tmp.getHostname()+", ip: "+NetUtil.byteIPAddrToString(tmp.getByteIPAddr()));
		}
		
	}
	
	
	
	
	/**
	 * parse all nodes
	 */
	public static List<String> parseAllNodes(String AllNodes, int port){
		
		List<String> AllAliveNodes = new ArrayList<String>(100);
	try {
		BufferedReader rf= new BufferedReader(new FileReader(new File(AllNodes)));
		if (rf == null) {
			//System.err.println("ERROR, empty file  !!");
			return AllAliveNodes ;
		} else {
			String cuLine;

			while (true) {

				cuLine = rf.readLine();
				
				//System.out.println("@: CurLine "+cuLine);
				if (cuLine == null) {
					//System.out.println("@: Panic ");
					break;
				}
				if (cuLine.startsWith("#")) {
					continue;
				}
				//trim the empty header
				cuLine=Matrix.trimEmpty(cuLine);
				if(cuLine.isEmpty()||cuLine==null){
					continue;
				}else{
				log.debug("$: current: "+cuLine);
				//AddressIF addr=AddressFactory.createUnresolved(cuLine,port);
				//log.info("$: real: "+addr.toString());
				AllAliveNodes.add(cuLine);
				}
				}
		}
		
		rf.close();
		PUtil.gc();
		
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
		
	//int len1=Math.min(Math.round(AllAliveNodes.size()*0.1f),10);
	//Collections.shuffle(AllAliveNodes);
	return AllAliveNodes;
	}
	
	/**
	 * translate the DNS name 
	 * @param AllNodes
	 * @param outFile
	 */
	public static void DNS2IPAddress(String AllNodes,final String outFile){
		int port =80;
		List<String> allnodes=parseAllNodes(AllNodes,port);
		AddressFactory.createResolved(allnodes,
				port, new CB1<Map<String, AddressIF>>() {
			
			protected void cb(CBResult result,
					Map<String, AddressIF> addrMap) {
			switch(result.state){
			case OK:{
				
				
				
				if(addrMap!=null&&!addrMap.isEmpty()){
					
					try {
						PrintWriter TestCaseStream = new PrintWriter(new BufferedOutputStream(new FileOutputStream(new File(
								outFile),true)));
						
						Iterator<String> ier = addrMap.keySet().iterator();	
						while(ier.hasNext()){
							String dnsName=ier.next();
							String IP=NetUtil.byteIPAddrToString(((NetAddress)addrMap.get(dnsName)).getByteIPAddr());
							TestCaseStream.append(dnsName+"\t"+IP+"\n");
							TestCaseStream.flush();
						}
						TestCaseStream.close();
						
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					

				}				
				break;
			}
			case TIMEOUT:
			case ERROR:{
				log.warn("can not parse the address");
				break;
			}
			
			}
			}		
		}
	);
	}
	
	
	
	
	public static boolean isWindows() {
		String osName = System.getProperty("os.name");
		if (osName.indexOf("Windows") >= 0 || osName.indexOf("windows") >= 0) {
			return true;
		} else {
			return false;
		}

	}

	
	
	public static boolean isLinux() {
		return !isWindows();
	}

	
	//Ericfu
	public static boolean
    validateAnIpAddressWithRegularExpression(String iPaddress){
      Pattern IP_PATTERN =
              Pattern.compile("^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])$");
    return IP_PATTERN.matcher(iPaddress).matches();
	}
	
	public static  void removeNonIP(List<String> nsList){
		if(nsList!=null&&!nsList.isEmpty()){
			Iterator<String> ier = nsList.iterator();
			while(ier.hasNext()){
				String tmp = ier.next();
				if(!validateAnIpAddressWithRegularExpression(tmp)){
					ier.remove();
				}
			}
		}
	}
	
	/**
	 * 
	 * ping timeout, 8 seconds in default
	 */
	
	
	
	/**
	 * ping a list of nodes
	 * @param list
	 */
	public static List<NodesPair>  pingTargetedNodes(final AddressIF me, final List list){
		
				// TODO Auto-generated method stub
				List<NodesPair> cachedMeasurements=new ArrayList<NodesPair>(5);
				
				Iterator<AddressIF> ier = list.iterator();
				try {
					
					while(ier.hasNext()){
						AddressIF target=ier.next();
						String ip=target.getHostname();
						if(ip==null||ip.isEmpty()){
							ip=NetUtil.byteIPAddrToString(((NetAddress)target).getByteIPAddr());
						}
						
						double rtt=MainGeneric.doPingDirect(ip);
						if(rtt>=0){
							cachedMeasurements.add(new NodesPair(me,target,rtt));	
						}else{
							log.debug("error incurs");
						}
						}
										
				} catch (Exception e) {
					
					e.printStackTrace();
				}
		return cachedMeasurements;
}
	
	
	public static double doPingDirect(final String ipToPing) {

		// not suitable for the KNN search process!,
		// [java] ER 1255106574621 EL :
		// java.util.ConcurrentModificationException
		// [java] java.lang.Exception: Stack trace
		// [java] at java.lang.Thread.dumpStack(Thread.java:1206)
		// [java] at edu.harvard.syrah.prp.Log.error(Log.java:181)
		// [java] at
		// edu.harvard.syrah.sbon.async.EL.handleSelectCallbacks(EL.java:836)
		// [java] at edu.harvard.syrah.sbon.async.EL.main(EL.java:434)
		// [java] at edu.NUDT.pdl.Nina.Ninaloader.main(Ninaloader.java:408)

				double rtt = -1;

				 //System.out.println("ipToPing: "+ipToPing);
				try {
					/*if (isWindows()) {

						Process p;

						p = Runtime.getRuntime().exec("ping -w 8000 -n 3 "+ipToPing);
						//System.out.println("@@");

						BufferedReader in = new BufferedReader(
								new InputStreamReader(p.getInputStream()));

						String line;

						int notReadyCount = 0;
						boolean notDone = true;

						while (notDone) {
							while (!in.ready()) {
								try {
									Thread.sleep(1 * 1000);
									notReadyCount++;
									if (notReadyCount > 4) {
										notDone = false;
										//System.out.println("@@");
										break;
									}
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}

							}
							notReadyCount = 0;
							if ((line = in.readLine()) == null) {
								notDone = false;
								break;
							}
							//System.out.println(line);
							// find RTT
							if (line.contains("Average =")
									|| line.contains("Æ½¾ù =")) {
								int beginIndex = line.lastIndexOf("= ") + 2;
								int endIndex = line.lastIndexOf("ms");
								String val = line.substring(beginIndex,
										endIndex);
								rtt = Double.parseDouble(val);
								notDone = false;
								break;
							}

						}
						// System.out.println("@@: "+rtt);
						// then destroy
						//close the opened file
						in.close();
						p.destroy();

					} else if (isLinux()) {*/
						//System.out.println("Is NonWindows!");
						//timout in 8 seconds, as default
						
						Process p;
						p = Runtime.getRuntime().exec(
								"ping -w 8 -i 0.21 -q -c 3 " + ipToPing);

						BufferedReader in = new BufferedReader(
								new InputStreamReader(p.getInputStream()));
						String line;

						while ((line = in.readLine()) != null) {
							// find RTT
							if (line.contains("rtt")
									|| line.contains("round-trip")) {
								String[] vals = line.split("/");
								rtt = Double.parseDouble(vals[4]);

								break;
							}
						}

						// then destroy
						in.close();
						p.destroy();
					//}
				} catch (IOException e) {
					System.err.println(e.toString());

				}
				return rtt;
	}
	
	
	/**
	 * synchronous ping based on windows or linux primitives
	 */
	public static void doPing(final String ipToPing, final CB1<Double> cbDone) {

		// not suitable for the KNN search process!,
		// [java] ER 1255106574621 EL :
		// java.util.ConcurrentModificationException
		// [java] java.lang.Exception: Stack trace
		// [java] at java.lang.Thread.dumpStack(Thread.java:1206)
		// [java] at edu.harvard.syrah.prp.Log.error(Log.java:181)
		// [java] at
		// edu.harvard.syrah.sbon.async.EL.handleSelectCallbacks(EL.java:836)
		// [java] at edu.harvard.syrah.sbon.async.EL.main(EL.java:434)
		// [java] at edu.NUDT.pdl.Nina.Ninaloader.main(Ninaloader.java:408)

		execMain.execute( new Runnable() {

			public void run() {
				double rtt = -1;

				// System.out.println("ipToPing: "+ipToPing);
				try {
					/*if (isWindows()) {

						Process p;

						p = Runtime.getRuntime().exec("ping -w 8000 -n 5 "+ipToPing);
						// System.out.println("@@");

						BufferedReader in = new BufferedReader(
								new InputStreamReader(p.getInputStream()));

						String line;

						int notReadyCount = 0;
						boolean notDone = true;

						while (notDone) {
							while (!in.ready()) {
								try {
									Thread.sleep(1 * 1000);
									notReadyCount++;
									if (notReadyCount > 4) {
										notDone = false;
										//System.out.println("@@");
										break;
									}
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}

							}
							notReadyCount = 0;
							if ((line = in.readLine()) == null) {
								notDone = false;
								break;
							}
							// find RTT
							if (line.contains("Average =")
									|| line.contains("Æ½ï¿½ï¿½ =")) {
								int beginIndex = line.lastIndexOf("= ") + 2;
								int endIndex = line.lastIndexOf("ms");
								String val = line.substring(beginIndex,
										endIndex);
								rtt = Double.parseDouble(val);
								notDone = false;
								break;
							}

						}
						// System.out.println("@@: "+rtt);
						// then destroy
						p.destroy();
						in.close();

					} else if (isLinux()) {*/
						//System.out.println("Is NonWindows!");

						Process p;
						p = Runtime.getRuntime().exec(
								"ping -w 8 -i 0.21 -q -c 3 " + ipToPing);

						BufferedReader in = new BufferedReader(
								new InputStreamReader(p.getInputStream()));
						String line;

						while ((line = in.readLine()) != null) {
							// find RTT
							if (line.contains("rtt")
									|| line.contains("round-trip")) {
								String[] vals = line.split("/");
								rtt = Double.parseDouble(vals[4]);

								break;
							}
						}

						// then destroy
						p.destroy();
						in.close();
					//}
				} catch (IOException e) {
					log.warn(e.toString());

				}

				cbDone.call(CBResult.OK(), rtt);

			}

		});

	}

	public static double ifSameSubsetThenDo(final AddressIF nodeA, final AddressIF nodeB){
		double latency=-1;
		Random r=new Random(System.currentTimeMillis());
		double eRTT=r.nextFloat()*3;
		String ANode=NetUtil.byteIPAddrToString(((NetAddress)nodeA).getByteIPAddr());
		String BNode=NetUtil.byteIPAddrToString(((NetAddress)nodeB).getByteIPAddr());
		
		String[] A=ANode.split("[., ]");
		String[] B=BNode.split("[., ]");
		
		//System.out.println(ANode+"\n"+BNode+"\n "+A.length+", "+B.length);
		if(A.length==B.length){
			//IPv4
			boolean same=true;
			//e.g. 192.168.1.X
			for(int i=0;i<3;i++){
				//System.out.println(A[i]+" <> "+B[i]);
				if(!A[i].equalsIgnoreCase(B[i])){
					same=false;
				}
			}
			
			if(same){
				return eRTT;
			}else{
				return latency;
			}
		}
		return latency;
		
	}
	
	public static void main(String[] args) {

		/*String ipToPing = args[0];*/

		// MainGeneric test=new MainGeneric ();
/*		MainGeneric.doPing(ipToPing, new CB1<Double>() {

			@Override
			protected void cb(CBResult result, Double lat) {
				// TODO Auto-generated method stub
				System.out.println("RTT: " + lat);
			}

		});*/
	/*	int port=55509;
		AddressIF A=AddressFactory.createUnresolved("192.168.1.7", port);
		AddressIF B=AddressFactory.createUnresolved("192.168.1.8", port);
		AddressIF C=AddressFactory.createUnresolved("192.168.2.14", port);
		
		List targets=new ArrayList(2);
		targets.add(A);
		targets.add(B);
		targets.add(C);
		
		AddressIF me=AddressFactory.createUnresolved("202.197.22.56", port);
		List<NodesPair>  result=pingTargetedNodes(me,targets);
		Iterator<NodesPair> ier = result.iterator();
		while(ier.hasNext()){
			System.out.println(ier.next().toString());
		}
		
		*/
		/*
		System.out.println(ifSameSubsetThenDo(A,B));
		System.out.println(ifSameSubsetThenDo(C,B));
		
		//double rtt=doPingDirect("192.168.1.85");
		//System.out.println(rtt);
		Set<AddressIF> nodes= parseAllNodes(args[0],80);
		 printAddress(nodes);
/*		String cuLine="192.168.1.85";
		int port=80;
		AddressIF addr=AddressFactory.createUnresolved(cuLine,port);
		log.info(addr.toString());*/
		
		
		//pase DNS
		//input name, output name
		if(args.length<2){
			usage();
		}
		DNS2IPAddress(args[0],args[1]);
		
		
	}
	
	
	public static void usage(){
		log.info("MainGeneric: DNSAddrFile, outPutFile");
		
	}
	
}
