package edu.harvard.syrah.sbon.async.comm.dns;

import java.util.StringTokenizer;

import edu.harvard.syrah.prp.Log;

/**
 * 
 * This abstract class defines all the base implementation details for all RR
 * types.
 * 
 * <pre>
 * 
 *  All RRs have the same top level format shown below:
 * 
 *  1  1  1  1  1  1
 *  0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5
 *  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 *  |                                               |
 *  /                                               /
 *  /                      NAME                     /
 *  |                                               |
 *  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 *  |                      TYPE                     |
 *  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 *  |                     CLASS                     |
 *  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 *  |                      TTL                      |
 *  |                                               |
 *  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 *  |                   RDLENGTH                    |
 *  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--|
 *  /                     RDATA                     /
 *  /                                               /
 *  +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 * 
 *  where:
 * 
 *  NAME            an owner name, i.e., the name of the node to which this
 *  resource record pertains.
 * 
 *  TYPE            two octets containing one of the RR TYPE codes.
 * 
 *  CLASS           two octets containing one of the RR CLASS codes.
 * 
 *  TTL             a 32 bit signed integer that specifies the time interval
 *  that the resource record may be cached before the source
 *  of the information should again be consulted.  Zero
 *  values are interpreted to mean that the RR can only be
 *  used for the transaction in progress, and should not be
 *  cached.  For example, SOA records are always distributed
 *  with a zero TTL to prohibit caching.  Zero values can
 *  also be used for extremely volatile data.
 * 
 *  RDLENGTH        an unsigned 16 bit integer that specifies the length in
 *  octets of the RDATA field.
 * 
 *  RDATA           a variable length string of octets that describes the
 *  resource.  The format of this information varies
 *  according to the TYPE and CLASS of the resource record.
 *  
 *  TYPE fields are used in resource records.  Note that these types are a
 *  subset of QTYPEs.
 * 
 *  TYPE            value and meaning
 * 
 *  A               1 a host address
 * 
 *  NS              2 an authoritative name server
 * 
 *  MD              3 a mail destination (Obsolete - use MX)
 * 
 *  MF              4 a mail forwarder (Obsolete - use MX)
 * 
 *  CNAME           5 the canonical name for an alias
 * 
 *  SOA             6 marks the start of a zone of authority
 * 
 *  MB              7 a mailbox domain name (EXPERIMENTAL)
 * 
 *  MG              8 a mail group member (EXPERIMENTAL)
 * 
 *  MR              9 a mail rename domain name (EXPERIMENTAL)
 * 
 *  NULL            10 a null RR (EXPERIMENTAL)
 * 
 *  WKS             11 a well known service description
 * 
 *  PTR             12 a domain name pointer
 * 
 *  HINFO           13 host information
 * 
 *  MINFO           14 mailbox or mail list information
 * 
 *  MX              15 mail exchange
 * 
 *  TXT             16 text strings
 * 
 * 
 *  QTYPE fields appear in the question part of a query.  QTYPES are a
 *  superset of TYPEs, hence all TYPEs are valid QTYPEs.  In addition, the
 *  following QTYPEs are defined:
 * 
 *  AXFR            252 A request for a transfer of an entire zone
 * 
 *  MAILB           253 A request for mailbox-related records (MB, MG or MR)
 * 
 *  MAILA           254 A request for mail agent RRs (Obsolete - see MX)
 * 
 *                255 A request for all records
 * 
 *  CLASS fields appear in resource records.  The following CLASS mnemonics
 *  and values are defined:
 * 
 *  IN              1 the Internet
 * 
 *  CS              2 the CSNET class (Obsolete - used only for examples in
 *  some obsolete RFCs)
 * 
 *  CH              3 the CHAOS class
 * 
 *  HS              4 Hesiod [Dyer 87]
 * 
 *  QCLASS fields appear in the question section of a query.  QCLASS values
 *  are a superset of CLASS values; every CLASS is a valid QCLASS.  In
 *  addition to CLASS values, the following QCLASSes are defined:
 * 
 *                255 any class
 * 
 * 
 * </pre>
 * 
 * <b>Logging:</b><br/> The static Logger instance,
 * <code>org.wonderly.net.dns.RR</code> is used for all standard RR
 * implementations.
 * <hr>
 * Copyright (c) 2001-2004, Gregg Wonderly All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * <p>
 * 
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 * <p>
 * 
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * <p>
 * 
 * Neither the name of Gregg Wonderly nor the names of contributors to this
 * software may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 * <p>
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * @author Gregg Wonderly - gregg.wonderly@pobox.com
 */
public abstract class RR {
	private static final Log log = new Log(RR.class);
	
	protected String name;
	protected int type;
	protected int rclass;
	protected int ttl;
	protected int rdlength;
	protected byte[] rdata;
	/** Offset in buffer where this entry was found */
	protected int off;
	protected static boolean debug;
	/** Logger instance for RR implementations to use */
	//protected static Logger log = Logger.getLogger(RR.class.getName());

	/**
	 * @deprecated - java.util.logging is now in use at FINE and lower levels for
	 *             debugging information
	 */
	public static void setDebug(boolean how) {
	}

	public String toString() {
		return DNSQuestion.toTime(ttl) + "\t\t"
				+ DNSQuestion.toClass(rclass) + "\t" + DNSQuestion.toType(type);
	}

	/**
	 * The the specific RR type value in the packet by skipping over the name.
	 */
	public static int getRRType(byte[] pkt, int off) {
		// Get the index past the name by passing null to cause a skip.
		int idx = extractName(pkt, off, pkt.length, null);
		//log.debug("extract type at idx: "+idx);
		return ((pkt[idx] & 0xff) << 8) | (pkt[idx + 1] & 0xff);
	}

	/**
	 * The the specific RR type value in the packet by skipping over the name.
	 */
	public static int getRRClass(byte[] pkt, int off) {
		// Get the index past the name by passing null to cause a skip.
		int idx = extractName(pkt, off, pkt.length, null);
		// log.fine("extract class at idx: "+idx);
		return ((pkt[idx + 2] & 0xff) << 8) | (pkt[idx + 3] & 0xff);
	}

	/**
	 * The the specific RR type value in the packet by skipping over the name.
	 */
	public static int getRRttl(byte[] pkt, int off) {
		// Get the index past the name by passing null to cause a skip.
		int idx = extractName(pkt, off, pkt.length, null);
		// log.fine("extract ttl at idx: "+idx);
		return ((pkt[idx + 4] & 0xff) << 24) | ((pkt[idx + 5] & 0xff) << 16)
				| ((pkt[idx + 6] & 0xff) << 8) | (pkt[idx + 7] & 0xff);
	}

	/**
	 * The the specific RR type value in the packet by skipping over the name.
	 */
	public static int getRRdlen(byte[] pkt, int off) {
		// Get the index past the name by passing null to cause a skip.
		int idx = extractName(pkt, off, pkt.length, null);
		// log.fine("extract dlen at idx: "+idx);
		return ((pkt[idx + 8] & 0xff) << 8) | (pkt[idx + 9] & 0xff);
	}

	/**
	 * Extracts a DNS name from a packet using the DNS convention of
	 * <code>len-byte bytes len-byte bytes len-byte bytes zero-byte</code>.
	 * 
	 * e.g. 3 www 7 cytetech 3 com 0
	 * 
	 * @param name
	 *          buffer for name, or null if you just want the first index past the
	 *          name.
	 */
	public static int extractName(byte[] data, int start, int len, StringBuffer name) {
		int idx = start;
		int tlen = 0;
		// log.fine("extract name ("+name+") at idx: "+idx+", data.length:
		// "+data.length );
		while (data[idx] != 0 && start < len) {
			if (name != null && name.length() > 0)
				name.append('.');
			++tlen;
			// log.finer( "["+idx+"]=0x"+Long.toHexString((data[idx]&0xff)));
			if ((data[idx] & 0xc0) == 0x0) {
				if (name != null) {
					// log.finer("in line label: @"+idx+": "+new String( data, idx+1,
					// data[idx] ));
					name.append(new String(data, idx + 1, data[idx]));
				}
				idx += (data[idx] & 0xff) + 1;
				tlen += (data[idx] & 0xff);
			} else if ((data[idx] & 0xc0) == 0xc0) {
				int off = (data[idx + 1] & 0xff) + ((data[idx] & 0x3f) << 8);
				StringBuffer buf = new StringBuffer();
				int toff = extractName(data, off, len, buf);
				if (name != null) {
					// log.finer("Ref'd label (have="+name+"): @"+off+": got: "+buf );
					name.append(buf.toString());
				}
				tlen += buf.toString().length();

				idx++;
				break;
			} else {
				throw new IllegalArgumentException("Unrecognized length format @" + idx + ": ("
						+ (data[idx] & 0xff) + ") 0x" + Long.toHexString(data[idx]));
			}

			// log.fine("next (0x"+Long.toHexString(data[idx]&0xff)+ "), idx: "+idx+",
			// data.length: "+data.length );
		}
		// log.fine("final name: "+name );
		return idx + 1;
	}

	public static int nameLength(String name) {
		return name.length() + 2;
	}

	protected int setParameters(byte[] pkt, int idx) {
		type = ((pkt[idx] & 0xff) << 8) | (pkt[idx + 1] & 0xff);
		idx += 2;
		rclass = ((pkt[idx] & 0xff) << 8) | (pkt[idx + 1] & 0xff);
		idx += 2;
		ttl = ((pkt[idx] & 0xff) << 24) | ((pkt[idx + 1] & 0xff) << 16) | ((pkt[idx + 2] & 0xff) << 8)
				| (pkt[idx + 3] & 0xff);
		idx += 4;
		rdlength = ((pkt[idx] & 0xff) << 8) | (pkt[idx + 1] & 0xff);
		idx += 2;
		return idx;
	}

	public static int copyInName(byte data[], int start, String name) {
		StringTokenizer st = new StringTokenizer(name, ".");
		int idx = 0;
		while (st.hasMoreTokens()) {
			String c = st.nextToken();
			if (c.length() > 63)
				c = c.substring(0, 63);
			data[idx++] = (byte) c.length();
			for (int i = 0; i < c.length(); ++i) {
				data[idx++] = (byte) c.charAt(i);
			}
		}
		data[idx] = 0;
		return idx + 1;
	}

	/**
	 * Parses an RR record header and calls the subclass processRecord method to
	 * parse the RR type specific data.
	 * 
	 * @param pkt
	 *          the complete response packet array.
	 * @param rroff
	 *          the offset for this particular RR, passed to
	 *          <code>processRecord(int)</code>
	 * 
	 * @see #processRecord(int)
	 */
	public RR(byte[] pkt, int rroff, int type) {
		rdata = pkt;
		this.type = type;

		// Get the initial name query.
		StringBuffer name = new StringBuffer();
		off = rroff;
		int idx = extractName(pkt, rroff, pkt.length, name);

		this.name = name.toString();

		// Set the parameters for the query.
		idx = setParameters(pkt, idx);
		processRecord(rroff);
	}

	public int getType() {
		return type;
	}

	/**
	 * Translates one of the TYPE_* values into a string that is the part after
	 * the TYPE_ part of the variable name.
	 */
	public static String toType(int type) {
		String buf = "" + type;
		switch (type) {
			case DNSQuestion.TYPE_A:
				buf = "A";
				break;
			case DNSQuestion.TYPE_NS:
				buf = "NS";
				break;
			case DNSQuestion.TYPE_MD:
				buf = "MD";
				break;
			case DNSQuestion.TYPE_MF:
				buf = "MF";
				break;
			case DNSQuestion.TYPE_CNAME:
				buf = "CNAME";
				break;
			case DNSQuestion.TYPE_SOA:
				buf = "SOA";
				break;
			case DNSQuestion.TYPE_MB:
				buf = "MB";
				break;
			case DNSQuestion.TYPE_MG:
				buf = "MG";
				break;
			case DNSQuestion.TYPE_MR:
				buf = "MR";
				break;
			case DNSQuestion.TYPE_NULL:
				buf = "NULL";
				break;
			case DNSQuestion.TYPE_WKS:
				buf = "WKS";
				break;
			case DNSQuestion.TYPE_PTR:
				buf = "PTR";
				break;
			case DNSQuestion.TYPE_HINFO:
				buf = "HINFO";
				break;
			case DNSQuestion.TYPE_MINFO:
				buf = "MINFO";
				break;
			case DNSQuestion.TYPE_MX:
				buf = "MX";
				break;
			case DNSQuestion.TYPE_TXT:
				buf = "TXT";
				break;
			case DNSQuestion.TYPE_AAAA:
				buf = "AAAA";
				break;
		}
		return buf;
	}

	/**
	 * Subclasses implement this method to parse out their RR type specific data.
	 * 
	 * @param rroff
	 *          the offset into this particular RRs data
	 */
	public abstract void processRecord(int rroff);

	/**
	 * Returns the length, in bytes, of the data this RR consumes.
	 * 
	 * Code using these RR objects should use this method to move to the next RR.
	 */
	public abstract int dataLength();
}
