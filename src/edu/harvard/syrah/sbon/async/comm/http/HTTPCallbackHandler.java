/*
 * @author Last modified by $Author: ledlie $
 * @version $Revision: 1.2 $ on $Date: 2009/03/27 17:39:57 $
 * @since Jul 15, 2005
 */
package edu.harvard.syrah.sbon.async.comm.http;

import java.util.Map;

import edu.harvard.syrah.sbon.async.CallbacksIF.CB2;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB7;
import edu.harvard.syrah.sbon.async.comm.AddressIF;

/**
 * Handles a HTTP request
 * 
 * @params remoteAddress
 * @params URI
 * @params requestData
 * 
 * @returns contentType
 * @returns reponseData
 * 
 * @author peter
 *
 */
public abstract class HTTPCallbackHandler extends
		CB7<AddressIF, String, String, Map<String,String>, Map<String,String>, String, CB2<String, byte[]>> {
	
	/* empty */
	
}
