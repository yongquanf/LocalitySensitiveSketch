/*
 * SBON
 * 
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.2 $ on $Date: 2007/08/14 11:18:14 $
 * @since Jan 5, 2005
 */
package edu.harvard.syrah.sbon.async.comm;

import java.net.InetAddress;


/**
 *
 * This interface defines a communication endpoint for the Comm Layer.
 *
 */
@SuppressWarnings("unchecked")
public interface AddressIF extends Comparable {
	
	public String getHostname();
	
	public int getPort();
	
	public boolean hasPort();
	
	public boolean isResolved();
	
	/**
	 * Determines if two addresses are the same.
	 * @param address Address for comparison
	 * @return True if addresses are the same.
	 */
	public boolean equals(Object obj);
	
	public String toString();
	
	public String toString(boolean colour);

	/**
	 * @return
	 */
	public byte[] getByteIPAddrPort();
	
	public int getIntIPAddr();
	
	public InetAddress getInetAddress();

}
