package edu.harvard.syrah.sbon.async.comm.obj;

import edu.harvard.syrah.sbon.async.comm.AddressIF;

public class simpleUDPResponseMsg extends ObjMessage {
	static final long serialVersionUID = 19121212123L;
	public AddressIF from;
	boolean ack=false;
	public simpleUDPResponseMsg(AddressIF _from) {

		from = _from;
	}
}
