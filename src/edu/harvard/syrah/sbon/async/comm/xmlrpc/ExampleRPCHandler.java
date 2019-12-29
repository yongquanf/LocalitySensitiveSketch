/*
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:40 $
 * @since Jan 11, 2005
 */
package edu.harvard.syrah.sbon.async.comm.xmlrpc;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.sbon.async.CBResult;
import edu.harvard.syrah.sbon.async.EL;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;


/**
 *
 */
public class ExampleRPCHandler {
	private static final Log log = new Log(ExampleRPCHandler.class);
	
	public void getStateName(int value, final CB1<Integer> cbInt) {
		log.info("Called getStateName with value=" + value);
		EL.get().registerTimerCB(5000, new CB0() {
			public void cb(CBResult result) {
				cbInt.call(CBResult.OK(), 16);
			}		
		});		
	}	
}
