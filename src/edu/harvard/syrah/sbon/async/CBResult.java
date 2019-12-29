/*
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:41 $
 * @since Jul 11, 2005
 */
package edu.harvard.syrah.sbon.async;

import java.io.Serializable;

public class CBResult implements Serializable {		
		
	static final long serialVersionUID = 1000000001L;
	
	public enum CBState {OK, ERROR, UNKNOWN, TIMEOUT}
	
	public CBState state;
	public String what;
	
	private CBResult(CBState state) { 
		this(state, null); 
	}
			
	private CBResult(CBState state, String what) { 
		this.state = state; 
		this.what = what;	
	}
	
	public static CBResult OK() { return new CBResult(CBState.OK); }
	public static CBResult OK(String what) { return new CBResult(CBState.OK, what); } 
	public static CBResult ERROR() { return new CBResult(CBState.ERROR); }
	public static CBResult ERROR(String what) { return new CBResult(CBState.ERROR, what); }
	public static CBResult ERROR(Exception e) { return new CBResult(CBState.ERROR, e.toString()); } 
	public static CBResult UNKNOWN() { return new CBResult(CBState.UNKNOWN); }
	public static CBResult UNKNOWN(String what) { return new CBResult(CBState.UNKNOWN, what); } 
	public static CBResult TIMEOUT() { return new CBResult(CBState.TIMEOUT); }
	public static CBResult TIMEOUT(String what) { return new CBResult(CBState.TIMEOUT, what); } 
			
	public String toString() {
		switch (state) {
			case OK 	   : return "OK";
			case ERROR   : return "ERROR(" + what + ")";
			case UNKNOWN : return "UNKNWON(" + what + ")";
			case TIMEOUT : return "TIMEOUT(" + what + ")";
			default 		 : return "?";
		}
	}
	
}
