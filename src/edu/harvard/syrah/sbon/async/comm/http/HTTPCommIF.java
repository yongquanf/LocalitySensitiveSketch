/*
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:41 $
 * @since Jul 13, 2005
 */
package edu.harvard.syrah.sbon.async.comm.http;

import edu.harvard.syrah.sbon.async.comm.TCPCommIF;

public interface HTTPCommIF extends TCPCommIF {

	public void checkHTTP(String url, boolean keepAlive, HTTPCB cbHTTPResponse);
	
	public void sendHTTPRequest(String url, String httpRequest, boolean keepAlive, HTTPCB cbHTTPResponse);	
	
	/*
	 * This class sends a http request for a stream. It assumes that the stream uses chunked encoding...
	 */	
	public void sendHTTPStreamRequest(String url, String httpRequest, HTTPCB cbHTTPResponse);
	
	public void registerHandler(String path, HTTPCallbackHandler httpRequestHandler);
	
	public void deregisterXMLRPCHandler(String path);
}
