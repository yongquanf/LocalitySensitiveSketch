package edu.harvard.syrah.sbon.async.comm.dns;

/**
This class provides the <code>TXT</code> RR record structure mapping.
<pre>

TXT RDATA format.

    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    /                   TXT-DATA                    /
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

where:

TXT-DATA        One or more <character-string>s.

TXT RRs are used to hold descriptive text.  The semantics of the text
depends on the domain where it is found.

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

public class TXTRR extends RR {
	protected String aname;
	protected String text;
	protected byte data[];
	protected int dlen;
	protected int sidx;
	
	public String toString() {
		if( text == null )
			text = new String(data);
		return aname+"\t\t"+super.toString()+"\t"+text;
	}
	
	public TXTRR( byte[]pkt, int cidx ) {
		super(pkt, cidx, DNSQuestion.TYPE_TXT );
	}
	
	public String getText( ) {
		if( text == null )
			text = new String(data);
		return text;
	}
	
	public int dataLength() {
		return dlen;
	}

	public void processRecord( int cidx ) {
		StringBuffer buf = new StringBuffer();
		sidx = cidx;
		int fcidx = cidx;
		cidx = RR.extractName( rdata, cidx, rdata.length, buf );
		aname = buf.toString();

		cidx = setParameters(rdata, cidx);
		
		buf.setLength(0);
		data = new byte[ rdlength ];
		int cnt = 0;
		while( cnt < rdlength ) {
			int l = rdata[cnt+cidx] & 0xff;
			for( int i = 0; i < l; ++i )
				buf.append( (char)rdata[cnt+cidx+1+i] );
			buf.append(' ');
			cnt += l + 1;
		}
		cidx += rdlength;
		data = buf.toString().getBytes();

		dlen = (cidx - fcidx);
	}
}
