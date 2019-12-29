package edu.harvard.syrah.sbon.async.comm.dns;

import edu.harvard.syrah.prp.Log;

/**
 * 
 * This class provides the <code>AAAA</code> RR record structure mapping.
 * 
 * 
 * @author Peter Pietzuch - prp@doc.ic.ac.uk
 */

public class AAAARR extends RR {
	private static final Log log = new Log(AAAARR.class);
	
	String host;
	String addrv6;
	byte[] ipv6;

	int dlen;

	public String toString() {
		return host + "\t\t" + super.toString() + "\t" + ipv6;
	}

	public String getIPv6Address() {
		return addrv6;
	}

	public AAAARR(byte[] pkt, int cidx) {
		super(pkt, cidx, DNSQuestion.TYPE_AAAA);
	}

	public byte[] getIPv6() {
		return ipv6;
	}

	public int dataLength() {
		return dlen;
	}

	public void processRecord(int cidx) {
		StringBuffer buf = new StringBuffer();
		int nmlen = cidx;
		int fidx = cidx;
		cidx = RR.extractName(rdata, cidx, rdata.length, buf);
		host = buf.toString();
		nmlen = cidx - nmlen;
		cidx = setParameters(rdata, cidx);
		int b1 = rdata[cidx++];
		int b2 = rdata[cidx++];
		int b3 = rdata[cidx++];
		int b4 = rdata[cidx++];
		int b5 = rdata[cidx++];
		int b6 = rdata[cidx++];
		int b7 = rdata[cidx++];
		int b8 = rdata[cidx++];
		int b9 = rdata[cidx++];
		int b10 = rdata[cidx++];
		int b11 = rdata[cidx++];
		int b12 = rdata[cidx++];
		int b13 = rdata[cidx++];
		int b14 = rdata[cidx++];
		int b15 = rdata[cidx++];
		int b16 = rdata[cidx++];

		addrv6 = (b1 & 0xff) + "." + (b2 & 0xff) + "." + (b3 & 0xff) + "." + (b4 & 0xff) + (b5 & 0xff)
				+ "." + (b6 & 0xff) + "." + (b7 & 0xff) + "." + (b8 & 0xff) 
				+ "." + (b9 & 0xff) + "." + (b10 & 0xff) + "." + (b11 & 0xff) + "." + (b12 & 0xff) 
				+ "." + (b13 & 0xff) + "." + (b14 & 0xff) + "." + (b14 & 0xff) + "." + (b16 & 0xff);
		ipv6 = new byte[] { (byte) (b1 & 0xff), (byte) (b2 & 0xff), (byte) (b3 & 0xff),
				(byte) (b4 & 0xff), (byte) (b5 & 0xff), (byte) (b6 & 0xff), (byte) (b7 & 0xff),
				(byte) (b8 & 0xff), (byte) (b9 & 0xff), (byte) (b10 & 0xff), (byte) (b11 & 0xff), 
				(byte) (b12 & 0xff), (byte) (b13 & 0xff), (byte) (b14 & 0xff), (byte) (b15 & 0xff), (byte) (b16 & 0xff)};
		dlen = cidx - fidx;
		//log.debug("AAAA dlen=" + dlen);
	}
}
