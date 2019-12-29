/*
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:40 $
 * @since Aug 10, 2005
 */
package edu.harvard.syrah.sbon.async.comm.dns;

import java.net.InetAddress;

import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;

/**
 * Performs an asynchronous DNS lookup
 *
 */
public interface DNSCommIF {
	
	void initServer(CB0 cbInit);
	
	void getHostByName(String hostName, CB1<InetAddress> cbInetAddress);
	
	void getIPAddrByName(String hostName, CB1<byte[]> cbIPAddr);
	
	void getNameByIPAddr(byte[] byteIPAddr, CB1<String> cbHostname);
	
	void updateCache(String hostname, byte[] byteIPAddr);
	
}
