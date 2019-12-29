/*
 * SBON
 * 
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:40 $
 * @since Jan 6, 2005
 */
package edu.harvard.syrah.sbon.async.comm.obj;

import edu.harvard.syrah.sbon.async.CallbacksIF.CB3;
import edu.harvard.syrah.sbon.async.comm.AddressIF;


/**
 *
 * Callback API for the Comm module.
 *
 */
public abstract class ObjCommRRCB<T extends ObjMessageIF> extends CB3<T, AddressIF, Long> {

  public ObjCommRRCB() { super(); }

  public ObjCommRRCB(String name) { super(name); }

  public ObjCommRRCB(long timeout) { super(timeout); }		
}
