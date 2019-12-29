/*
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:41 $
 * @since Mar 29, 2007
 */
package edu.harvard.syrah.sbon.async.comm;

import java.net.InetAddress;

public class SimAddr implements AddressIF {

	private String name;
	
	public SimAddr(String name) {
		this.name = name;
	}
	
	public byte[] getByteIPAddrPort() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getHostname() {
		return name;
	}

	public InetAddress getInetAddress() {
		return null;
	}

	public int getIntIPAddr() {
		return 0;
	}

	public int getPort() {
		return 0;
	}

	public boolean hasPort() {
		return false;
	}

	public boolean isResolved() {
		return false;
	}

	public String toString(boolean colour) {
		return name;
	}

	public int compareTo(Object o) {
		SimAddr cmpAddr = (SimAddr) o;
		return name.compareTo(cmpAddr.name);
	}
	
	@Override
	public String toString() {
		return toString(true);
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		SimAddr cmpAddr = (SimAddr) obj;
		return name.equals(cmpAddr.name);
	}
	

}
