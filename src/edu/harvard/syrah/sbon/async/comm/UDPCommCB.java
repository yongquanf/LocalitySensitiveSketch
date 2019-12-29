/*
 * SBON
 * 
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:41 $
 * @since Jan 19, 2005
 */
package edu.harvard.syrah.sbon.async.comm;

import java.nio.ByteBuffer;

import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB4;


/**
 *
 */
public abstract class UDPCommCB extends CB4<ByteBuffer, AddressIF, Long, CB1<Boolean>> {
 
	public UDPCommCB() { super(); }	
	
	public UDPCommCB(long timeout) { super(timeout); }
		
}
