/*
 * SBON
 * 
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:40 $
 * @since Jan 6, 2005
 */
package edu.harvard.syrah.sbon.async.comm.obj;

import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB4;
import edu.harvard.syrah.sbon.async.comm.AddressIF;


/**
 *
 * Callback API for the Comm module.
 *
 */
public abstract class ObjCommCB<T extends ObjMessageIF> extends CB4<T, AddressIF, Long, CB1<Boolean>> {

  public ObjCommCB() { super(); }

  public ObjCommCB(String name) { super(name); }

  public ObjCommCB(long timeout) { super(timeout); }		
}
