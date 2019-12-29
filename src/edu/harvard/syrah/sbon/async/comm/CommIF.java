/*
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:41 $
 * @since Jan 19, 2005
 */
package edu.harvard.syrah.sbon.async.comm;

import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;


/**
 *
 */
public interface CommIF {
		
	/**
	 * Initialises the comm module with a local address.
	 * 
	 * @param localAddress
	 */
	public void initServer(AddressIF localAddress, CB0 cbInit);
	
	public void deinitServer();
		
	/**
	 * Returns the address that this instance of the comm module considers to be local
	 * @return
	 */
	public AddressIF getLocalAddress();
	
	public void setLocalAddress(AddressIF localAddress);
	
	public int getLocalPort();

}
