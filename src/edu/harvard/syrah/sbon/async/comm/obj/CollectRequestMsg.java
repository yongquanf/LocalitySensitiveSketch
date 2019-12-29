package edu.harvard.syrah.sbon.async.comm.obj;

import edu.harvard.syrah.sbon.async.comm.AddressIF;
import edu.harvard.syrah.sbon.async.comm.obj.ObjMessage;

public class CollectRequestMsg extends ObjMessage {

	static final long serialVersionUID = 19L;
	public AddressIF from;

	public CollectRequestMsg(AddressIF _from) {

		from = _from;
	};
}
