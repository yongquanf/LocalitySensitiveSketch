package edu.harvard.syrah.sbon.async.comm.dns;

import edu.harvard.syrah.prp.Log;

/**
 *
This class provides the <code>A</code> RR record structure mapping.

<pre>
A RDATA format.

    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    |                    ADDRESS                    |
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

where:

ADDRESS         A 32 bit Internet address.

Hosts that have multiple Internet addresses will have multiple A
records.

A records cause no additional section processing.  The RDATA section of
an A line in a master file is an Internet address expressed as four
decimal numbers separated by dots without any imbedded spaces (e.g.,
"10.2.0.52" or "192.0.5.6").

</pre>

<hr>
Copyright (c) 2001-2004, Gregg Wonderly
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

public class ARR extends RR {
	private static final Log log = new Log(ARR.class);
	
	String host;
	String addr;
	byte[] byteAddr;
	
	int recs = 1;
	int dlen;
	
	public String toString() {
		return host+"\t\t"+super.toString()+"\t"+addr;
	}
	
	public ARR( byte[]pkt, int cidx ) {
		super(pkt, cidx, DNSQuestion.TYPE_A );
	}

	public String getHost() {
		return host;
	}
	
	public String getIPAddress() {
		return addr;
	}
	
	public byte[] getByteIPAddress() {
		return byteAddr;
	}
	
	public int getRecordCount() {
		return recs;
	}
	
	public int dataLength() {
		return dlen;
	}

	public void processRecord( int cidx ) {
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
		addr =(b1 & 0xff) + "." + (b2 & 0xff) + "." + (b3 & 0xff) + "." + (b4 & 0xff);
		byteAddr = new byte[] {(byte) (b1 & 0xff), (byte) (b2 & 0xff), (byte) (b3 & 0xff), (byte) (b4 & 0xff)};
		dlen = cidx - fidx;
		//log.debug("A dlen=" + dlen);
	}
}
