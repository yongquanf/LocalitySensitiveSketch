package edu.harvard.syrah.sbon.async.comm.dns;

/**
This class provides the <code>MG</code> RR record structure mapping.
<pre>
MG RDATA format (EXPERIMENTAL).

    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    /                   MGMNAME                     /
    /                                               /
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

where:

MGMNAME         A <domain-name> which specifies a mailbox which is a
                member of the mail group specified by the domain name.

MG records cause no additional section processing.

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

public class MGRR extends CNAMERR {
	String emailbx;
		
	public MGRR( byte[]pkt, int cidx ) {
		super(pkt, cidx);
	}
	
	public String getRMailBX() {
		return getCName();
	}
	
	public String getEMailBX() {
		return emailbx;
	}

	public void processRecord( int cidx ) {
		int fcidx = cidx;
		super.processRecord( cidx );
		cidx += dlen;
//		StringBuffer buf = new StringBuffer();
//		cidx = RR.extractName( rdata, cidx, rdata.length, buf );
//		emailbx = buf.toString();
		dlen = (cidx - fcidx);
	}
}
