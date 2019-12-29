package edu.harvard.syrah.sbon.async.comm.dns;


/**
This class provides the <code>NS</code> RR record structure mapping.
<pre>
NS RDATA format

    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    /                   NSDNAME                     /
    /                                               /
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

where:

NSDNAME         A <domain-name> which specifies a host which should be
                authoritative for the specified class and domain.

NS records cause both the usual additional section processing to locate
a type A record, and, when used in a referral, a special search of the
zone in which they reside for glue information.

The NS RR states that the named host should be expected to have a zone
starting at owner name of the specified class.  Note that the class may
not indicate the protocol family which should be used to communicate
with the host, although it is typically a strong hint.  For example,
hosts which are name servers for either Internet (IN) or Hesiod (HS)
class information are normally queried using IN class protocols.

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

public class NSRR extends RR {
	String host;
	String domain;
	int recs = 1;
	int dlen;
	
	public String toString() {
		return domain+"\t\t"+super.toString()+"\t"+host;
	}
	
	public NSRR( byte[]pkt, int cidx ) {
		super(pkt, cidx, DNSQuestion.TYPE_NS );
	}

	public String getHost() {
		return host;
	}
	
	public int getRecordCount() {
		return recs;
	}
	
	public int dataLength() {
		return dlen;
	}

	public void processRecord( int cidx ) {
		StringBuffer buf = new StringBuffer();
		int dnlen = cidx;
		cidx = RR.extractName( rdata, cidx, rdata.length, buf );
		domain = buf.toString();

		dnlen = cidx - dnlen;
		cidx = setParameters( rdata, cidx );

		int nmlen = cidx;
		buf.setLength(0);
		cidx = RR.extractName( rdata, cidx, rdata.length, buf );
		host = buf.toString();

		nmlen = cidx - nmlen;
		dlen = 2 + 2 + 4 + 2 + nmlen + dnlen;
//		System.out.println("cidx: "+cidx);
	}
}
