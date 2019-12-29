/*
 * SBON
 * 
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:41 $
 * @since Jan 19, 2005
 */
package edu.harvard.syrah.sbon.async.comm;

import java.nio.ByteBuffer;

import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;


/**
 *
 * Interface for the UDPComm module that implements UDP packet communication
 *
 */
public interface UDPCommIF extends CommIF {

	public void initServer(AddressIF bindAddress, CB0 cbInit);
	
	public void sendPacket(ByteBuffer data, AddressIF destAddr);
	
	public void sendPacket(ByteBuffer data, AddressIF destAddr, UDPCommCB cb);
	
	public void registerPacketCallback(UDPCommCB cb);
	
	public void deregisterPacketCallback(UDPCommCB cb);
	
}
