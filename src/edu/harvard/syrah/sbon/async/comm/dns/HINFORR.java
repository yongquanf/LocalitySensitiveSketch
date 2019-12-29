package edu.harvard.syrah.sbon.async.comm.dns;

/**
This class provides the <code>HINFO</code> RR record structure mapping.
<pre>
HINFO RDATA format.

    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    /                    CPU                        /
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    /                    OS                         /
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

where:

CPU             A <character-string> which specifies the CPU type.

OS              A <character-string> which specifies the operating
                system type.

Standard values for CPU and OS can be found in [RFC 1010]. 

HINFO records are used to acquire general information about a host.
The main use is for protocols such as FTP that can use special 
procedures when talking between machines or operating systems of
the same type. 


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

public class HINFORR extends RR {
	String aname;
	String cpu;
	String os;
	int dlen;
	int sidx;
	
	public String toString() {
		return aname+"\t\t"+super.toString()+"\t"+cpu+" running "+os;
	}
	
	public HINFORR( byte[]pkt, int cidx ) {
		super(pkt, cidx, DNSQuestion.TYPE_HINFO );
	}
	
	public String getCPU( ) {
		return cpu;
	}
	
	public String getOS( ) {
		return os;
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
		int len = rdata[cidx++] & 0xff;
		for( int i = 0; i < len; ++i )
			buf.append( (char)rdata[i+cidx] );
		
		cidx += len;
		cpu = buf.toString();

		buf.setLength(0);
		len = rdata[cidx++] & 0xff;
		for( int i = 0; i < len; ++i )
			buf.append( (char)rdata[i+cidx] );
		os = buf.toString();
		cidx += len;

		dlen = (cidx - fcidx);
	}
}
