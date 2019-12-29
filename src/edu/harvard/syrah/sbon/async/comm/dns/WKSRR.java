package edu.harvard.syrah.sbon.async.comm.dns;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Enumeration;
import java.util.Vector;

/**
This class provides the <code>WKS</code> RR record structure mapping.
<pre>

WKS RDATA format.

           +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
           | 0  1  2  3  4  5 6 ADDRESS   10 11 12 13 14 15|
           +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
           |       PROTOCOL        |                       |
           +--+--+--+--+--+--+--+--+                       |
           |                                               |
           /                   <BIT MAP>                   /
           /                                               /
           +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

      where:

      ADDRESS   - An 32 bit ARPA Internet address

      PROTOCOL  - An 8 bit IP protocol number

      <BIT MAP> - A variable length bit map.  The bit map must be a
                multiple of 8 bits long.

      The WKS record is used to describe the well known services
      supported by a particular protocol on a particular internet
      address.  The PROTOCOL field specifies an IP protocol number, and
      the bit map has one bit per port of the specified protocol.  The
      first bit corresponds to port 0, the second to port 1, etc.  If
      less than 256 bits are present, the remainder are assumed to be
      zero.  The appropriate values for ports and protocols are
      specified in [13].

      For example, if PROTOCOL=TCP (6), the 26th bit corresponds to TCP
      port 25 (SMTP).  If this bit is set, a SMTP server should be
      listening on TCP port 25; if zero, SMTP service is not supported
      on the specified address.

      The anticipated use of WKS RRs is to provide availability
      information for servers for TCP and UDP.  If a server supports
      both TCP and UDP, or has multiple Internet addresses, then
      multiple WKS RRs are used.

      WKS RRs cause no additional section processing.  The RDATA section
      of a WKS record consists of a decimal protocol number followed by
      mnemonic identifiers which specify bits to be set to 1.

</pre>

<hr>
Copyright (c) 2001-2005, Gregg Wonderly
All rights reserved.
<p>
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
<p>

Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer. 
<p>

Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution. 
<p>

Neither the name of Gregg Wonderly nor the names of contributors to this
software may be used to endorse or promote products derived from this
software without specific prior written permission. 
<p>

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
THE POSSIBILITY OF SUCH DAMAGE.
 *
 *  @author Gregg Wonderly - gregg.wonderly@pobox.com
 */

public class WKSRR extends RR {
	String host;
	String addr;
	int recs = 1;
	int dlen;
	int proto;
	Vector<Integer> ports;
	
	public String toString() {
		return host+"\t\t"+super.toString()+"\t"+addr+", proto="+proto+", ports="+ports;
	}
	
	public WKSRR( byte[]pkt, int cidx ) {
		super(pkt, cidx, DNSQuestion.TYPE_WKS );
	}

	public String getHost() {
		return host;
	}
	
	public InetAddress getHostAddress() throws IOException {
		return InetAddress.getByName( addr );
	}

	public int getRecordCount() {
		return recs;
	}

	public int dataLength() {
		return dlen;
	}

	@SuppressWarnings("unchecked")
	public Enumeration getPortsList() {
		return ports.elements();
	}

	public void processRecord( int cidx ) {
		ports = new Vector<Integer>(10);
		StringBuffer buf = new StringBuffer();
		int nmlen = cidx;
		int fidx = cidx;
		cidx = RR.extractName( rdata, cidx, rdata.length, buf );
		host = buf.toString();
		nmlen = cidx - nmlen;
		cidx = setParameters( rdata, cidx );
		int b1 = rdata[cidx++];
		int b2 = rdata[cidx++];
		int b3 = rdata[cidx++];
		int b4 = rdata[cidx++];
		addr =(b1&0xff)+"."+
			(b2&0xff)+"."+
			(b3&0xff)+"."+
			(b4&0xff);
		proto = rdata[cidx++] & 0xff;
		int cnt = rdlength - 5;
		for( int i = 0; i < cnt; ++i ) {
			int v = rdata[i + cidx] & 0xff;
//			System.out.println("data["+(i+cidx)+"]: "+v );
			for( int j = 7; j >= 0; --j ) {
				int bit = (i * 8) + (7-j);
//				System.out.println( "bit no: "+bit+", set? "+(v & (1<<j)));
				if( (v & (1<<j)) != 0 ) {
					ports.addElement( new Integer( bit ) );
				}
			}
		}
		dlen = (cidx - fidx) + cnt;
	}
}
