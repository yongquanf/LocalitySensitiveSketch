package edu.harvard.syrah.sbon.async.comm.obj;

import edu.harvard.syrah.sbon.async.comm.AddressIF;

public class simpleUDPRequestMsg extends ObjMessage {
	static final long serialVersionUID = 19121212123L;
	public AddressIF from;

	public simpleUDPRequestMsg(AddressIF _from) {

		from = _from;
	};
}
