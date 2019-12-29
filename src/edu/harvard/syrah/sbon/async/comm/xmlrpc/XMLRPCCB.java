/*
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:40 $
 * @since Jan 11, 2005
 */
package edu.harvard.syrah.sbon.async.comm.xmlrpc;

import edu.harvard.syrah.sbon.async.CBResult;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;


/**
 *
 */
public abstract class XMLRPCCB extends CB1<Object> {
	
	protected abstract void cb(CBResult result, Object httpResponse);

}
