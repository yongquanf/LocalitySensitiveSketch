/*
 * SBON
 * 
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:41 $
 * @since Jan 12, 2005
 */
package edu.harvard.syrah.sbon.async.comm;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.sbon.async.Barrier;
import edu.harvard.syrah.sbon.async.CBResult;
import edu.harvard.syrah.sbon.async.EL;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;
import edu.harvard.syrah.sbon.async.comm.dns.DNSComm;
import edu.harvard.syrah.sbon.async.comm.dns.DNSCommIF;

/**
 * 
 * Factory classes for addresses
 * 
 */
public class AddressFactory {
	protected static final Log log = new Log(AddressFactory.class);

	private static DNSCommIF dnsComm;

	static {
		dnsComm = new DNSComm();
		// DNSServer will dynamically listen to the right port
		dnsComm.initServer(new CB0() {
			protected void cb(CBResult result) {
				switch (result.state) {
					case TIMEOUT:
					case ERROR: {
						log.error(result.toString());
						break;
					}
				}
			}
		});
	}
	
	// Copy constructor
	public static AddressIF create(AddressIF addr) {
		if (!(addr instanceof NetAddress)) {
			log.error("Can only copy a NetAddress: " + addr);
		}
		NetAddress copyAddr = (NetAddress) addr;
		return new NetAddress(copyAddr.hostname, copyAddr.byteIPAddr, copyAddr.port, null);
	}

	public static AddressIF createUnresolved(String addressString) {
		return new NetAddress(addressString, null, NetAddress.UNKNOWN_PORT, null);
	}

	public static AddressIF createUnresolved(String hostname, int port) {
		return new NetAddress(hostname, null, port, null);
	}

	public static void createResolved(String hostname, int port, CB1<AddressIF> cbAddress) {
		new NetAddress(hostname, null, port, cbAddress);
	}

	public static void createResolved(String addressString, CB1<AddressIF> cbAddress) {
		new NetAddress(addressString, cbAddress);
	}

	public static void createResolved(List<String> addressStrings,  int port,
			final CB1<Map<String, AddressIF>> cbAddressMap) {
		final Barrier lookupBarrier = new Barrier(true);
		final Map<String, AddressIF> addressMap = new HashMap<String, AddressIF>();
		lookupBarrier.setNumForks(addressStrings.size());
		for (final String addressString : addressStrings) {
			//lookupBarrier.fork();
			new NetAddress(addressString, port, new CB1<AddressIF>() {
				protected void cb(CBResult result, AddressIF address) {
					switch (result.state) {
						case OK: {
							addressMap.put(addressString, address);
							// log.debug("addressMap.keySet.size()=" +
							// addressMap.keySet().size());
							break;
						}
						case TIMEOUT:
						case ERROR: {
							log.warn("Could not resolve: address=" + address + " result=" + result);
							break;
						}
					}
					lookupBarrier.join();
				}
			});
		}
		EL.get().registerTimerCB(lookupBarrier, new CB0() {
			protected void cb(CBResult result) {
				cbAddressMap.call(CBResult.OK(), addressMap);
			}
		});
	}
	
	public static void createResolved(String[] addressStrings, final CB1<Map<String, AddressIF>> cbAddressMap) {
		createResolved(Arrays.asList(addressStrings), cbAddressMap);
	}
		
	public static void createResolved(List<String> addressStrings,
		final CB1<Map<String, AddressIF>> cbAddressMap) {
		final Barrier lookupBarrier = new Barrier(true);
		final Map<String, AddressIF> addressMap = new HashMap<String, AddressIF>();
		lookupBarrier.setNumForks(addressStrings.size());
		for (final String addressString : addressStrings) {
			//lookupBarrier.fork();
			new NetAddress(addressString, new CB1<AddressIF>() {
				protected void cb(CBResult result, AddressIF address) {
					switch (result.state) {
						case OK: {
							addressMap.put(addressString, address);
							// log.debug("addressMap.keySet.size()=" +
							// addressMap.keySet().size());
							break;
						}
						case TIMEOUT:
						case ERROR: {
							log.warn("Could not resolve: address=" + address + " result=" + result);
							break;
						}
					}
					lookupBarrier.join();
				}
			});
		}
		EL.get().registerTimerCB(lookupBarrier, new CB0() {
			protected void cb(CBResult result) {
				cbAddressMap.call(CBResult.OK(), addressMap);
			}
		});
	}

	public static void createResolved(byte[] byteIPAddr, int port, CB1<AddressIF> cbAddress) {
		new NetAddress(null, byteIPAddr, port, cbAddress);
	}

	public static void createResolved(SocketAddress socketAddress, CB1<AddressIF> cbAddress) {
		new NetAddress(null, ((InetSocketAddress) socketAddress).getAddress().getAddress(),
				((InetSocketAddress) socketAddress).getPort(), cbAddress);
	}

	public static void createResolved(InetAddress inetAddress, int port, CB1<AddressIF> cbAddress) {
		new NetAddress(null, inetAddress.getAddress(), port, cbAddress);
	}

	public static void createResolved(byte[] byteIPAddrPort, CB1<AddressIF> cbAddress) {
		ByteBuffer bb = ByteBuffer.wrap(byteIPAddrPort);
		byte[] byteIPAddr = new byte[4];
		bb.get(byteIPAddr);
		int port = bb.getInt();
		new NetAddress(null, byteIPAddr, port, cbAddress);
	}

	public static AddressIF createLocalhost(int port) {
		return new NetAddress("localhost", new byte[] { 127, 0, 0, 1 }, port, null);
	}

	public static AddressIF createLocal(int port) {
		try {
			return new NetAddress(port);
		} catch (UnknownHostException e) {
			log.error("Local host unknown? " + e);
		}
		return null;
	}
	
	public static AddressIF createLocalAddress() {
		return createLocal(NetAddress.UNKNOWN_PORT);
	}

	public static AddressIF createServer(int port) {
		return new NetAddress(null, new byte[] { 0, 0, 0, 0 }, port, null);
	}

	public static AddressIF create(AddressIF address, int port) {
		NetAddress netAddress = (NetAddress) address;
		return new NetAddress(netAddress.getHostname(), netAddress.getByteIPAddr(), port, null);
	}

	static DNSCommIF getDNSComm() {
		if (dnsComm == null)
			log.error("No DNSComm service created. Cannot do lookups.");
		return dnsComm;
	}

}
