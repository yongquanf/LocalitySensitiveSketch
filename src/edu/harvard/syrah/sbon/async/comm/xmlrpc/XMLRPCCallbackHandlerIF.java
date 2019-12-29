/*
 * SBON
 * 
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.2 $ on $Date: 2007/08/14 11:18:13 $
 * @since Jan 12, 2005
 */
package edu.harvard.syrah.sbon.async.comm.xmlrpc;

import java.util.Vector;

import org.apache.xmlrpc.XmlRpcHandler;

import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;

/**
 *
 * This is a new type of XMLRPCHandler that is asynchronous and returns the result with a 
 * callback object.
 *
 */
public interface XMLRPCCallbackHandlerIF extends XmlRpcHandler {

	@SuppressWarnings("unchecked")
	public void execute (String method, Vector params, CB1<Object> cbObject) throws Exception;
	
}
