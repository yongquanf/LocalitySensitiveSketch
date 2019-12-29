/*
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:40 $
 * @since Aug 10, 2005
 */
package edu.harvard.syrah.sbon.async.comm.dns;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.regex.Pattern;

import sun.net.dns.ResolverConfiguration;
import util.async.MainGeneric;

//import edu.NUDT.pdl.Nina.util.MainGeneric;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.prp.NetUtil;
import edu.harvard.syrah.prp.POut;
import edu.harvard.syrah.prp.PUtil;
import edu.harvard.syrah.sbon.async.CBResult;
import edu.harvard.syrah.sbon.async.EL;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;
import edu.harvard.syrah.sbon.async.EL.Priority;
import edu.harvard.syrah.sbon.async.comm.*;

/**
 * Communication module for DNS
 * 
 */
public class DNSComm extends UDPComm implements DNSCommIF {
	protected static final Log log = new Log(DNSComm.class);

	private static final long START_DNS_TIMEOUT = 1000; // 1000 ms

	private Map<String, Integer> dnsCache = new HashMap<String, Integer>();
	private Map<Integer, String> reverseDNSCache = new HashMap<Integer, String>();

	int MAX_DNS_RETRY_COUNT = 10;

	private Set<AddressIF> dnsServerList;

	public DNSComm() {
		super(Priority.NORMAL);
		log.debug("Initialising DNSComm...");
	}

	

	
	@SuppressWarnings("unchecked")
	public void initServer(CB0 cbInit) {
		List<String> nsList = ResolverConfiguration.open().nameservers();
		
		//find DNS servers Ericfu
		MainGeneric.removeNonIP(nsList);
		log.info("dnsServers=" + POut.toString(nsList));

		dnsServerList = new HashSet<AddressIF>();
		int i;
		
		for (i = 0; i < nsList.size(); i++) {
			dnsServerList.add(AddressFactory.createUnresolved(nsList.get(i), 53));
		}

		log.debug("Initialising DNSComm with " + i + " DNS servers: " + dnsServerList);

		// dnsServer = AddressFactory.createUnresolved("192.168.5.10", 53);
		// Bind this to the wildcard address - does this mean that we'll see all UDP
		// packets?
		// AddressIF localBindAddress = AddressFactory.createServer(0);
		// super.initServer(localBindAddress, cbInit);
	}

	public void getHostByName(final String hostname, final CB1<InetAddress> cbInetAddress) {
		getIPAddrByName(hostname, new CB1<byte[]>() {
			protected void cb(CBResult result, byte[] byteIPAddr) {
				switch (result.state) {
					case OK: {
						InetAddress ipAddr = null;
						try {
							ipAddr = InetAddress.getByAddress(hostname, byteIPAddr);
						} catch (UnknownHostException e) {
							log.error("Unknown host after IP address resolution? " + e);
						}
						cbInetAddress.call(result, ipAddr);
						break;
					}
					case TIMEOUT: {
						cbInetAddress.call(CBResult.TIMEOUT("DNS timeout for host=" + hostname), null);
						break;
					}
					case ERROR: {
						cbInetAddress.call(result, null);
						break;
					}
				}
			}
		});
	}

	public void getIPAddrByName(final String hostname, final CB1<byte[]> cbByteIPAddr) {
		// log.debug("getIPAddrByName.hostname=" + hostname);

		// Check the cache
		final byte[] cachedByteIPAddr = lookupCacheHostname(hostname);
		if (cachedByteIPAddr != null) {
			log.debug("DNS cache hit: cachedByteIPAddr=" + NetUtil.byteIPAddrToString(cachedByteIPAddr));
			EL.get().registerTimerCB(new CB0() {
				protected void cb(CBResult result) {
					cbByteIPAddr.call(CBResult.OK(), cachedByteIPAddr);
				}
			});
			return;
		}

		doQueryByName(hostname, cbByteIPAddr, 0);
	}

	public AddressIF getDNSServer() {
		return PUtil.getRandomObject(dnsServerList);
	}

	public void doQueryByName(final String hostname, final CB1<byte[]> cbByteIPAddr,
			final int queryCount) {

		DNSQuestion aQuestion = new DNSQuestion(hostname, DNSQuestion.TYPE_A, DNSQuestion.CLASS_IN);
		DNSQuery q = new DNSQuery(new DNSQuestion[] { aQuestion });

		final AddressIF dnsServer = getDNSServer();

		log.debug("Sending DNS query to " + dnsServer);
		long dnsTimeout = START_DNS_TIMEOUT * (queryCount + 1);
		q.runQuery(this, dnsServer, new CB1<DNSQuery>(dnsTimeout) {
			protected void cb(CBResult result, DNSQuery query) {
				switch (result.state) {
					case OK: {
						byte[] byteIPAddr = null;

						for (RR rr : query.getAnswers()) {
							if (rr instanceof ARR) {
								ARR arr = (ARR) rr;
								byteIPAddr = arr.byteAddr;
							}
						}

						if (byteIPAddr != null) {
							updateCache(hostname, byteIPAddr);
							cbByteIPAddr.call(CBResult.OK(), byteIPAddr);
						} else {
							log.debug("Unknown host=" + hostname);
							cbByteIPAddr.call(CBResult.ERROR("Unknown host: " + hostname), null);
						}
						break;
					}

					case ERROR: {
						if (queryCount < MAX_DNS_RETRY_COUNT) {
							log.warn("Error retry DNS query for " + hostname + " " + (queryCount + 1) + " times");
							doQueryByName(hostname, cbByteIPAddr, queryCount + 1);
							return;
						}
						cbByteIPAddr.call(CBResult.ERROR("Could not complete query"), null);
						break;
					}
					case TIMEOUT: {
						if (queryCount < MAX_DNS_RETRY_COUNT) {
							log.debug("Timeout retry DNS query for " + hostname + " " + (queryCount + 1)
									+ " times");
							doQueryByName(hostname, cbByteIPAddr, queryCount + 1);
							return;
						}

						log.warn("Timeout DNS query retry limit for host=" + hostname);
						cbByteIPAddr.call(CBResult.TIMEOUT("No response from DNS for host=" + hostname), null);
						break;
					}
				}
			}
		});
	}

	public void getNameByIPAddr(final byte[] byteIPAddr, final CB1<String> cbHostname) {
		// log.debug("getNameByIPAddr.byteIPAddr=" +
		// NetUtil.byteIPAddrToString(byteIPAddr));

		// Check the cache
		String cachedHostname = lookupCacheByteIPAddr(byteIPAddr);
		if (cachedHostname != null) {
			log.debug("Reverse DNS cache hit: cachedHostname=" + cachedHostname);
			cbHostname.call(CBResult.OK(), cachedHostname);
			return;
		}
		doQueryByAddr(byteIPAddr, cbHostname, 0);
	}

	public void doQueryByAddr(final byte[] byteIPAddr, final CB1<String> cbHostname,
			final int queryCount) {
		String queryString = NetUtil.reverseByteIPAddrToString(byteIPAddr) + ".in-addr.arpa";
		DNSQuestion aQuestion = new DNSQuestion(queryString, DNSQuestion.TYPE_PTR, DNSQuestion.CLASS_IN);
		DNSQuery q = new DNSQuery(new DNSQuestion[] { aQuestion });

		// log.debug("q.requestId=" + q.getQueryId());

		final AddressIF dnsServer = getDNSServer();

		log.debug("Sending reverse DNS query to " + dnsServer);
		long dnsTimeout = START_DNS_TIMEOUT * (queryCount + 1);
		q.runQuery(this, dnsServer, new CB1<DNSQuery>(dnsTimeout) {

			protected void cb(CBResult result, DNSQuery query) {
				String hostname = null;
				switch (result.state) {
					case OK: {
						for (RR rr : query.getAnswers()) {
							if (rr instanceof PTRRR) {
								PTRRR ptrrr = (PTRRR) rr;
								hostname = ptrrr.ptr;
								break;
							}
						}
						updateCache(hostname, byteIPAddr);
						cbHostname.call(CBResult.OK(), hostname);
						break;
					}
					case ERROR: {
						if (queryCount < MAX_DNS_RETRY_COUNT) {
							log.warn("Error retry DNS query for " + NetUtil.byteIPAddrToString(byteIPAddr) + " "
									+ (queryCount + 1) + " times");
							doQueryByAddr(byteIPAddr, cbHostname, queryCount + 1);
							return;
						}
						hostname = NetUtil.byteIPAddrToString(byteIPAddr);
						updateCache(hostname, byteIPAddr);
						cbHostname.call(CBResult.ERROR("Reverse DNS lookup failed"), hostname);
						break;
					}
					case TIMEOUT: {
						if (queryCount < MAX_DNS_RETRY_COUNT) {
							log.debug("Timeout retry DNS query for " + NetUtil.byteIPAddrToString(byteIPAddr)
									+ " " + (queryCount + 1) + " times");
							doQueryByAddr(byteIPAddr, cbHostname, queryCount + 1);
							return;
						}

						hostname = NetUtil.byteIPAddrToString(byteIPAddr);
						updateCache(hostname, byteIPAddr);
						log.warn("Timeout DNS reverse query retry limit for ip=" + hostname);
						cbHostname.call(CBResult.TIMEOUT("Reverse DNS timed out for ip=" + hostname), hostname);
						break;
					}
				}
			}
		});
	}

	public void updateCache(String hostname, byte[] byteIPAddr) {
		if (hostname == null) {
			hostname = NetUtil.byteIPAddrToString(byteIPAddr);
		}

		int intIPAddr = NetUtil.byteIPToIntIP(byteIPAddr);
		if (!dnsCache.containsKey(hostname) || !reverseDNSCache.containsKey(intIPAddr)) {
			log.debug("Updating DNS cache:" + hostname + "->" + NetUtil.byteIPAddrToString(byteIPAddr));
			dnsCache.put(hostname, intIPAddr);
			reverseDNSCache.put(intIPAddr, hostname);
		}
	}

	private byte[] lookupCacheHostname(String hostname) {
		Integer intIP = dnsCache.get(hostname);
		return (intIP != null) ? NetUtil.intIPToByteIP(intIP) : null;
	}

	private String lookupCacheByteIPAddr(byte[] byteIPAddr) {
		return reverseDNSCache.get(NetUtil.byteIPToIntIP(byteIPAddr));
	}

}
