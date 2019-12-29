/*
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:41 $
 * @since Jan 11, 2005
 */
package edu.harvard.syrah.sbon.async.comm.http;

import edu.harvard.syrah.sbon.async.CBResult;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB3;


/**
 *
 */
public abstract class HTTPCB extends CB3<Integer, String, String> {
	
	public HTTPCB() { super(); }
	
	public HTTPCB(long timeout) { super(timeout);	}
	
	protected abstract void cb(CBResult result, Integer resultCode, String requestResponse, String httpData);

}
