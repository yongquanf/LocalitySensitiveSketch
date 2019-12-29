/*
 * SBON
 * 
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:40 $
 * @since Jan 9, 2005
 */
package edu.harvard.syrah.sbon.async.comm.xmlrpc;

import java.net.MalformedURLException;
import java.net.UnknownHostException;

import edu.harvard.syrah.sbon.async.comm.http.HTTPCommIF;


/**
 *
 * API for Communication layer using XMLRPC
 *
 */
public interface XMLRPCCommIF extends HTTPCommIF {

	public void call(String urlString, String methodName, Object... arguments) 
		throws MalformedURLException, UnknownHostException;
	
	public void call(String urlString, String methodName, XMLRPCCB cbResult, Object... arguments) 
		throws MalformedURLException, UnknownHostException;
	
	public void registerXMLRPCHandler(String handlerName, Object handler);
	
	public void deregisterXMLRPCHandler(String handlerName);
	
}
