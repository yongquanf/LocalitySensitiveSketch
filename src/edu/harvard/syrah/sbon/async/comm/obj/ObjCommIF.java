/*
 * SBON
 * 
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.2 $ on $Date: 2007/08/14 11:18:13 $
 * @since Jan 5, 2005
 */
package edu.harvard.syrah.sbon.async.comm.obj;

import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;
import edu.harvard.syrah.sbon.async.comm.AddressIF;
import edu.harvard.syrah.sbon.async.comm.TCPCommIF;



/**
 *
 * API for the Communication layer using object serialisation
 *
 */
public interface ObjCommIF extends TCPCommIF {
		
	/**
	 * Sends a new message to a destination address. This will reuse existing connections.
   *
	 * @param message
	 * @param destAddr
	 */	
	public void sendMessage(ObjMessageIF message, AddressIF destAddr, CB0 cbSent);
	
	public void sendMessage(ObjMessageIF message, AddressIF destAddr, boolean bestEffort, CB0 cbSent);
	
	/**
	 * Sends a request/reply message.
	 * 
	 * @param message
	 * @param destAddr
	 * @param cbResponseMessage
	 */
	public void sendRequestMessage(ObjMessageIF message, AddressIF destAddr, 
		ObjCommRRCB<? extends ObjMessageIF> cbResponseMessage);
	
	/**
	 * Sends a request/reply message and also registers a handler for an error response.
	 * 
	 * @param message
	 * @param destAddr
	 * @param cbResponseMessage
	 * @param cbErrorMessage
	 */
	public void sendRequestMessage(ObjMessageIF message, AddressIF destAddr, 
		ObjCommRRCB<? extends ObjMessageIF> cbResponseMessage, ObjCommRRCB<? extends ObjMessageIF> cbErrorMessage);
		
	public void sendResponseMessage(ObjMessageIF message, AddressIF destAddr, long requestMsgId, CB0 cbSent);
	
	public void sendErrorMessage(ObjMessageIF message, AddressIF destAddr, long requestMsgId, CB0 cbSent); 
	
	/**
	 * Enables objects to register their interest in incoming messages. The callback method will
	 * be invoked if a message of class messageClass is received.
	 * 
	 * @param messageClass
	 * @param callback
	 */	
	@SuppressWarnings("unchecked")
	public void registerMessageCB(Class messageClass, ObjCommCB<? extends ObjMessageIF> cb);
	
	@SuppressWarnings("unchecked")
	public void deregisterMessageCB(Class messageClass, ObjCommCB<? extends ObjMessageIF> cb);
}
