package edu.harvard.syrah.sbon.async.comm.obj;

import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB4;
import edu.harvard.syrah.sbon.async.comm.AddressIF;


public abstract class ObjUDPCommCB<T extends ObjMessageIF> extends CB4<T, AddressIF, Long, CB1<Boolean>> {

	  public ObjUDPCommCB() { super(); }

	  public ObjUDPCommCB(String name) { super(name); }

	  public ObjUDPCommCB(long timeout) { super(timeout); }		
	}