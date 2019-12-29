package edu.harvard.syrah.sbon.async.comm.dns;

/**
This class provides the <code>MX</code> RR record structure mapping.
<pre>
MX RDATA format.

    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    |                  PREFERENCE                   |
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    /                   EXCHANGE                    /
    /                                               /
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

where:

PREFERENCE      A 16 bit integer which specifies the preference given to
                this RR among others at the same owner.  Lower values
                are preferred.

EXCHANGE        A <domain-name> which specifies a host willing to act as
                a mail exchange for the owner name.

MX records cause type A additional section processing for the host
specified by EXCHANGE.  The use of MX RRs is explained in detail in
[RFC-974].
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

public class MXRR extends RR {
	int pref;
	String mxname;
	String mxfor;
	int dlen;
	int sidx;
	
	public String toString() {
		return mxfor+"\t\t"+super.toString()+"\t "+pref+" "+mxname;
	}
	
	public MXRR( byte[]pkt, int cidx ) {
		super(pkt, cidx, DNSQuestion.TYPE_MX );
	}

	public int getPreference() {
		return pref;
	}
	
	public String getExchanger() {
		return mxname;
	}
	
	public int dataLength() {
		return dlen;
	}

	public void processRecord( int cidx ) {
		StringBuffer buf = new StringBuffer();
		sidx = cidx;
		int fcidx = cidx;
		cidx = RR.extractName( rdata, cidx, rdata.length, buf );
		mxfor = buf.toString();

		cidx = setParameters(rdata, cidx);

		pref = ((rdata[cidx]&0xff)<<8) | (rdata[cidx+1]&0xff);
		cidx += 2; // preference
		
		buf.setLength(0);
		cidx = RR.extractName( rdata, cidx, rdata.length, buf );
		dlen = (cidx - fcidx);
		mxname = buf.toString();
	}
}
