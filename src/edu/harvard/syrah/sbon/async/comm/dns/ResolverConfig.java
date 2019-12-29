package edu.harvard.syrah.sbon.async.comm.dns;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import sun.net.dns.ResolverConfiguration;
import util.async.MainGeneric;

public class ResolverConfig {

	

	//private String [] servers = null;
	
	Set<String> servers=new HashSet<String>(2);

	private static ResolverConfig currentConfig;

	static {
		refresh();
	}

	public
	ResolverConfig() {
	/*	if (findProperty())
			return;
		if (findSunJVM())
			return;*/
		if (servers == null || servers.isEmpty()) {
			String OS = System.getProperty("os.name");
			String vendor = System.getProperty("java.vendor");
			System.out.println(OS);
			System.out.println(vendor);
		/*	if (OS.indexOf("Windows") != -1) {
				if (OS.indexOf("95") != -1 ||
				    OS.indexOf("98") != -1 ||
				    OS.indexOf("ME") != -1)
					find95();
				else
					findNT();
			} else if (OS.indexOf("NetWare") != -1) {
				findNetware();
			} else if (vendor.indexOf("Android") != -1) {
				findAndroid();
			} else {*/ 
			
				findUnix();
				System.out.println(servers.toString());
			//}
		}
	}

	private void
	addServer(String server, List list) {
		if(server.matches("[a-zA-Z]")){
			System.out.println(server);
		}
		if (list.contains(server))
			return;
/*		if (Options.check("verbose"))
			System.out.println("adding server " + server);*/
		list.add(server);
	}

/*	private void
	addSearch(String search, List list) {
		Name name;
		if (Options.check("verbose"))
			System.out.println("adding search " + search);
		try {
			name = Name.fromString(search, Name.root);
		}
		catch (TextParseException e) {
			return;
		}
		if (list.contains(name))
			return;
		list.add(name);
	}*/

	/*private void
	configureFromLists(List lserver, List lsearch) {
		if (servers == null && lserver.size() > 0)
			servers = (String []) lserver.toArray(new String[0]);
		if (searchlist == null && lsearch.size() > 0)
			searchlist = (Name []) lsearch.toArray(new Name[0]);
	}*/

	/**
	 * Looks in the system properties to find servers and a search path.
	 * Servers are defined by dns.server=server1,server2...
	 * The search path is defined by dns.search=domain1,domain2...
	 */
	private boolean
	findProperty() {
		String prop;
		List lserver = new ArrayList(0);
		List lsearch = new ArrayList(0);
		StringTokenizer st;

		prop = System.getProperty("dns.server");
		if (prop != null) {
			st = new StringTokenizer(prop, ",");
			while (st.hasMoreTokens())
				addServer(st.nextToken(), lserver);
		}

		/*prop = System.getProperty("dns.search");
		if (prop != null) {
			st = new StringTokenizer(prop, ",");
			while (st.hasMoreTokens())
				addSearch(st.nextToken(), lsearch);
		}
		configureFromLists(lserver, lsearch);*/
		return (servers != null && !servers.isEmpty());
	}

	/**
	 * Uses the undocumented Sun DNS implementation to determine the configuration.
	 * This doesn't work or even compile with all JVMs (gcj, for example).
	 */
	private boolean
	findSunJVM() {
		List lserver = new ArrayList(0);
		List lserver_tmp;
		List lsearch = new ArrayList(0);
		List lsearch_tmp;

		try {
			Class [] noClasses = new Class[0];
			Object [] noObjects = new Object[0];
			String resConfName = "sun.net.dns.ResolverConfiguration";
			Class resConfClass = Class.forName(resConfName);
			Object resConf;

			// ResolverConfiguration resConf = ResolverConfiguration.open();
			Method open = resConfClass.getDeclaredMethod("open", noClasses);
			resConf = open.invoke(null, noObjects);

			// lserver_tmp = resConf.nameservers();
			Method nameservers = resConfClass.getMethod("nameservers",
								    noClasses);
			lserver_tmp = (List) nameservers.invoke(resConf, noObjects);

			// lsearch_tmp = resConf.searchlist();
			Method searchlist = resConfClass.getMethod("searchlist",
								    noClasses);
			lsearch_tmp = (List) searchlist.invoke(resConf, noObjects);
		}
		catch (Exception e) {
			return false;
		}

		if (lserver_tmp.size() == 0)
			return false;

		if (lserver_tmp.size() > 0) {
			Iterator it = lserver_tmp.iterator();
			while (it.hasNext())
				addServer((String) it.next(), lserver);
		}

	/*	if (lsearch_tmp.size() > 0) {
			Iterator it = lsearch_tmp.iterator();
			while (it.hasNext())
				addSearch((String) it.next(), lsearch);
		}
		configureFromLists(lserver, lsearch);*/
		return true;
	}

	/**
	 * Looks in /etc/resolv.conf to find servers and a search path.
	 * "nameserver" lines specify servers.  "domain" and "search" lines
	 * define the search path.
	 */
	private void
	findResolvConf(String file) {
		InputStream in = null;
		try {
			in = new FileInputStream(file);
		}
		catch (FileNotFoundException e) {
			return;
		}
		InputStreamReader isr = new InputStreamReader(in);
		BufferedReader br = new BufferedReader(isr);
		List lserver = new ArrayList(0);
		List lsearch = new ArrayList(0);
		try {
			String line;
			while ((line = br.readLine()) != null) {
				if (line.startsWith("nameserver")) {
					StringTokenizer st = new StringTokenizer(line);
					st.nextToken(); /* skip nameserver */
					addServer(st.nextToken(), lserver);
				}
				/*else if (line.startsWith("domain")) {
					StringTokenizer st = new StringTokenizer(line);
					st.nextToken();  skip domain 
					if (!st.hasMoreTokens())
						continue;
					if (lsearch.isEmpty())
						addSearch(st.nextToken(), lsearch);
				}
				else if (line.startsWith("search")) {
					if (!lsearch.isEmpty())
						lsearch.clear();
					StringTokenizer st = new StringTokenizer(line);
					st.nextToken();  skip search 
					while (st.hasMoreTokens())
						addSearch(st.nextToken(), lsearch);
				}*/
			}
			br.close();
		}
		catch (IOException e) {
		}

		//configureFromLists(lserver, lsearch);
	}

	private void
	findUnix() {
		findResolvConf("/etc/resolv.conf");
	}

	private void
	findNetware() {
		findResolvConf("sys:/etc/resolv.cfg");
	}

	/**
	 * Parses the output of winipcfg or ipconfig.
	 *//*
	private void
	findWin(InputStream in) {
		String packageName = ResolverConfig.class.getPackage().getName();
		String resPackageName = packageName + ".windows.DNSServer";
		ResourceBundle res = ResourceBundle.getBundle(resPackageName);

		String host_name = res.getString("host_name");
		String primary_dns_suffix = res.getString("primary_dns_suffix");
		String dns_suffix = res.getString("dns_suffix");
		String dns_servers = res.getString("dns_servers");

		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		try {
			List lserver = new ArrayList();
			List lsearch = new ArrayList();
			String line = null;
			boolean readingServers = false;
			boolean readingSearches = false;
			while ((line = br.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(line);
				if (!st.hasMoreTokens()) {
					readingServers = false;
					readingSearches = false;
					continue;
				}
				String s = st.nextToken();
				if (line.indexOf(":") != -1) {
					readingServers = false;
					readingSearches = false;
				}
				
				if (line.indexOf(host_name) != -1) {
					while (st.hasMoreTokens())
						s = st.nextToken();
					Name name;
					try {
						name = Name.fromString(s, null);
					}
					catch (TextParseException e) {
						continue;
					}
					if (name.labels() == 1)
						continue;
					addSearch(s, lsearch);
				} else if (line.indexOf(primary_dns_suffix) != -1) {
					while (st.hasMoreTokens())
						s = st.nextToken();
					if (s.equals(":"))
						continue;
					addSearch(s, lsearch);
					readingSearches = true;
				} else if (readingSearches ||
					   line.indexOf(dns_suffix) != -1)
				{
					while (st.hasMoreTokens())
						s = st.nextToken();
					if (s.equals(":"))
						continue;
					addSearch(s, lsearch);
					readingSearches = true;
				} else if (readingServers ||
					   line.indexOf(dns_servers) != -1)
				{
					while (st.hasMoreTokens())
						s = st.nextToken();
					if (s.equals(":"))
						continue;
					addServer(s, lserver);
					readingServers = true;
				}
			}
			
			configureFromLists(lserver, lsearch);
		}
		catch (IOException e) {
		}
		finally {
			try {
				br.close();
			}
			catch (IOException e) {
			}
		}
		return;
	}

	*//**
	 * Calls winipcfg and parses the result to find servers and a search path.
	 *//*
	private void
	find95() {
		String s = "winipcfg.out";
		try {
			Process p;
			p = Runtime.getRuntime().exec("winipcfg /all /batch " + s);
			p.waitFor();
			File f = new File(s);
			findWin(new FileInputStream(f));
			new File(s).delete();
		}
		catch (Exception e) {
			return;
		}
	}

	*//**
	 * Calls ipconfig and parses the result to find servers and a search path.
	 *//*
	private void
	findNT() {
		try {
			Process p;
			p = Runtime.getRuntime().exec("ipconfig /all");
			findWin(p.getInputStream());
			p.destroy();
		}
		catch (Exception e) {
			return;
		}
	}
*/
	/**
	 * Parses the output of getprop, which is the only way to get DNS
	 * info on Android. getprop might disappear in future releases, so
	 * this code comes with a use-by date.
	 */
	private void
	findAndroid() {
		String re1 = "^\\d+(\\.\\d+){3}$";
		String re2 = "^[0-9a-f]+(:[0-9a-f]*)+:[0-9a-f]+$";
		try { 
			ArrayList maybe = new ArrayList(); 
			String line; 
			Process p = Runtime.getRuntime().exec("getprop"); 
			InputStream in = p.getInputStream();
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			while ((line = br.readLine()) != null ) { 
				StringTokenizer t = new StringTokenizer( line, ":" );
				String name = t.nextToken();
				if (name.indexOf( ".dns" ) > -1) {
					String v = t.nextToken();
					v = v.replaceAll( "[ \\[\\]]", "" );
					if ((v.matches(re1) || v.matches(re2)) &&
					    !maybe.contains(v))
						maybe.add(v);
				}
			}
			//configureFromLists(maybe, null);
		} catch ( Exception e ) { 
			// ignore resolutely
		}
	}
/*
	*//** Returns all located servers *//*
	public String []
	servers() {
		return servers;
	}

	*//** Returns the first located server *//*
	public String
	server() {
		if (servers == null)
			return null;
		return servers[0];
	}

	*//** Returns all entries in the located search path *//*
	public Name []
	searchPath() {
		return searchlist;
	}
*/
	/** Gets the current configuration */
	public static synchronized ResolverConfig
	getCurrentConfig() {
		return currentConfig;
	}

	/** Gets the current configuration */
	public static void
	refresh() {
		ResolverConfig newConfig = new ResolverConfig();
		synchronized (ResolverConfig.class) {
			currentConfig = newConfig;
		}
		
		System.out.println( validateAnIpAddressWithRegularExpression("None"));
		System.out.println( validateAnIpAddressWithRegularExpression("202.197.22.11"));
	
List<String> nsList = ResolverConfiguration.open().nameservers();
        System.out.println(nsList);
		//find DNS servers Ericfu
		MainGeneric.removeNonIP(nsList);
		
		System.out.println(nsList);
	}
	static boolean
    validateAnIpAddressWithRegularExpression(String iPaddress){
      Pattern IP_PATTERN =
              Pattern.compile("^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])$");
    return IP_PATTERN.matcher(iPaddress).matches();
}
	
	
	
	
	public static void main(String[] args){
		refresh() ;
	}
	
}
