package util.async;

import java.io.Serializable;
import java.util.Vector;

import edu.harvard.syrah.sbon.async.comm.AddressFactory;
import edu.harvard.syrah.sbon.async.comm.AddressIF;

public class NodesPair<T> implements Serializable {

	static final long serialVersionUID = 1000000001L;

	final static protected int CLASS_HASH = NodesPair.class.hashCode();

	public T startNode;
	public T endNode;
	public T fatherNode;
	public double rtt;
	public double elapsedTime=-1;
	public int ElapsedSearchHops=-1;

	public int totalSendingMessage=-1;

	NodesPair() {
	}

	public NodesPair(T _startNode, T _endNode, double _rtt) {
		startNode =_startNode;
		endNode = _endNode;
		rtt = _rtt;
		fatherNode=null;
	}

	public NodesPair(T _startNode, T _endNode, double _rtt, double _elapsedTime) {
		startNode =_startNode;
		endNode = _endNode;
		rtt = _rtt;
		fatherNode=null;
		elapsedTime=_elapsedTime;
	}
	
	public NodesPair(T _startNode, T _endNode, double _rtt, T _fatherNode,int _elapsedSearchHops) {
		startNode =_startNode;
		endNode =_endNode;
		fatherNode=_fatherNode;
		rtt = _rtt;
		ElapsedSearchHops=_elapsedSearchHops;
	}
	
	public NodesPair(T _startNode, T _endNode, double _rtt,  double _elapsedTime,int _elapsedSearchHops) {
		startNode =_startNode;
		endNode =_endNode;
		elapsedTime=_elapsedTime;
		rtt = _rtt;
		ElapsedSearchHops=_elapsedSearchHops;
	}
	public NodesPair(T _startNode, T _endNode, double _rtt,  double _elapsedTime,int _elapsedSearchHops,int _totalSendingMessage) {
		startNode =_startNode;
		endNode =_endNode;
		elapsedTime=_elapsedTime;
		rtt = _rtt;
		ElapsedSearchHops=_elapsedSearchHops;
		 totalSendingMessage= _totalSendingMessage;
	}
	
	public NodesPair(T _startNode, T _endNode, double _rtt, T _fatherNode, double _elapsedTime,int _elapsedSearchHops) {
		startNode =_startNode;
		endNode =_endNode;
		fatherNode=_fatherNode;
		rtt = _rtt;
		elapsedTime=_elapsedTime;
		ElapsedSearchHops=_elapsedSearchHops;
	}
	
	public NodesPair(T _startNode, T _endNode, double _rtt, T _fatherNode) {
		startNode =_startNode;
		endNode =_endNode;
		fatherNode=_fatherNode;
		rtt = _rtt;
	}
	
	
	/**
	 * omit the father node
	 * @param _startNode
	 * @param _rtt
	 * @param _fatherNode
	 */
	public NodesPair(T _startNode, double _rtt, T _fatherNode) {
		startNode = _startNode;
		endNode = null;
		fatherNode=_fatherNode;
		rtt = _rtt;
	}
	
	
	
	void setRTT(double _rtt) {
		rtt = _rtt;
		return;
	}

	/*
	 * public void toSerialized(DataOutputStream dos) throws IOException {
	 * 
	 * dos.writeByte(version); for (int i = 0; i < num_dims; ++i) { // when
	 * writing, cast to float dos.writeFloat((float) coords[i]); } // if
	 * (VivaldiClient.USE_HEIGHT) dos.writeFloat((float) coords[num_dims]); }
	 */
	public NodesPair makeCopy() {
		NodesPair cpy = new NodesPair(startNode, endNode, rtt,fatherNode);
		cpy.elapsedTime=this.elapsedTime;
		return cpy;
	}

	@Override
	public int hashCode() {
		int hc = CLASS_HASH;
		if(startNode!=null){
		hc ^= startNode.hashCode();
		}
		if(endNode!=null){
		hc ^= endNode.hashCode();
		}
		return hc;
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		//remove && Math.abs(this.rtt-tmp.rtt)<0.002
		
		NodesPair tmp = (NodesPair) obj;
		if(this.startNode!=null&&this.endNode!=null){
			if (this.startNode.equals(tmp.startNode)
				&& this.endNode.equals(tmp.endNode)) {
				return true;
			}
		}else if(this.startNode!=null&&tmp.startNode!=null){
			if (this.startNode.equals(tmp.startNode)) {
					return true;
				}
		}
		return false;
		
	}

	@Override
	public String toString() {
		final StringBuffer sbuf = new StringBuffer(1024);
		sbuf.append("(");
		sbuf.append(startNode.toString());
		sbuf.append(",");
		if(endNode!=null){
		sbuf.append(endNode.toString());
		sbuf.append(",");
		}
		sbuf.append(rtt);
		sbuf.append(")");

		return sbuf.toString();

	}

	public String SimtoString() {
		final StringBuffer sbuf = new StringBuffer(1024);
		/*
		sbuf.append("(");
		sbuf.append(startNode.toString());
		sbuf.append(": ");
		sbuf.append(rtt);
		sbuf.append(")");
		*/
		
		
		sbuf.append(startNode.toString()+" "+rtt+" Hops: "+this.ElapsedSearchHops);		
		
		return sbuf.toString();

	}
	
	public static void main(String[]args){
		NodesPair m=new NodesPair(AddressFactory.createUnresolved("192.168.1.45",56),AddressFactory.createUnresolved("192.168.1.46",58),-2);
		Vector<NodesPair> vec=new Vector<NodesPair>(1);
		vec.add(m);
		NodesPair m1=new NodesPair(AddressFactory.createUnresolved("192.168.1.45",56),AddressFactory.createUnresolved("192.168.1.46",58),-2);
		
		if(vec.contains(m1)){
			System.out.println("Yes, equal works!");
		}else{
			System.err.println("!");
		}
		Vector<AddressIF> vec2=new Vector<AddressIF>(2);
		AddressIF t1 = AddressFactory.createUnresolved("192.168.1.45",56);
		vec2.add(t1);
		AddressIF t2 = AddressFactory.createUnresolved("192.168.1.45",56);
		if(vec2.contains(t2)){
			System.out.println("Yes, equal works!");
		}else{
			System.err.println("!");
		}
		
	}
}