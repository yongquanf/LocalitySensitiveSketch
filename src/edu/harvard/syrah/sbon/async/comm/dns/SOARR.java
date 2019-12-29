package edu.harvard.syrah.sbon.async.comm.dns;


/**
This class provides the <code>SOA</code> RR record structure mapping.
<pre>
SOA RDATA format.

    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    /                     MNAME                     /
    /                                               /
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    /                     RNAME                     /
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    |                    SERIAL                     |
    |                                               |
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    |                    REFRESH                    |
    |                                               |
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    |                     RETRY                     |
    |                                               |
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    |                    EXPIRE                     |
    |                                               |
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    |                    MINIMUM                    |
    |                                               |
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

where:

MNAME           The <domain-name> of the name server that was the
                original or primary source of data for this zone.

RNAME           A <domain-name> which specifies the mailbox of the
                person responsible for this zone.

SERIAL          The unsigned 32 bit version number of the original copy
                of the zone.  Zone transfers preserve this value.  This
                value wraps and should be compared using sequence space
                arithmetic.

REFRESH         A 32 bit time interval before the zone should be
                refreshed.

RETRY           A 32 bit time interval that should elapse before a
                failed refresh should be retried.

EXPIRE          A 32 bit time value that specifies the upper limit on
                the time interval that can elapse before the zone is no
                longer authoritative.

MINIMUM         The unsigned 32 bit minimum TTL field that should be
                exported with any RR from this zone.

SOA records cause no additional section processing.

All times are in units of seconds.

Most of these fields are pertinent only for name server maintenance
operations.  However, MINIMUM is used in all query operations that
retrieve RRs from a zone.  Whenever a RR is sent in a response to a
query, the TTL field is set to the maximum of the TTL field from the RR
and the MINIMUM field in the appropriate SOA.  Thus MINIMUM is a lower
bound on the TTL field for all RRs in a zone.  Note that this use of
MINIMUM should occur when the RRs are copied into the response and not
when the zone is loaded from a master file or via a zone transfer.  The
reason for this provison is to allow future dynamic update facilities to
change the SOA RR with known semantics.

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

public class SOARR extends RR {
	String rname;
	String mname;
	String dname;
	int recs = 1;
	int dlen;
	int serial;
	int refresh;
	int retry;
	int expire;
	int minimum;
	
	public String toString() {
		return dname+"\t\t"+super.toString()+"\t"+mname+", "+rname+"\n"+
			"	SERIAL "+serial+"\n"+
			"	REFRESH "+DNSQuestion.toTime(refresh)+"\n"+
			"	RETRY "+DNSQuestion.toTime(retry)+"\n"+
			"	EXPIRE "+DNSQuestion.toTime(expire)+"\n"+
			"	MINIMUM "+DNSQuestion.toTime(minimum)
			;
	}
	
	public SOARR( byte[]pkt, int cidx ) {
		super(pkt, cidx, DNSQuestion.TYPE_SOA);
	}

	public String getNSHost() {
		return mname;
	}

	public String getResponsibleEmail() {
		return rname;
	}
	
	public int getRecordCount() {
		return recs;
	}
	
	public int dataLength() {
		return dlen;
	}

	public void processRecord( int cidx ) {
		StringBuffer buf = new StringBuffer();
		int fidx = cidx;
		
		cidx = extractName( rdata, cidx, rdata.length, buf );
		dname = buf.toString();
		cidx = setParameters( rdata, cidx );
		buf.setLength(0);
		cidx = extractName( rdata, cidx, rdata.length, buf );
		mname = buf.toString();
		buf.setLength(0);
		cidx = RR.extractName( rdata, cidx, rdata.length, buf );
		rname = buf.toString();
		
		serial = ((rdata[cidx]&0xff) << 24) | ((rdata[cidx+1] & 0xff)<<16) |
			((rdata[cidx+2]&0xff) << 8) | (rdata[cidx+3] & 0xff);
		cidx += 4;

		refresh = ((rdata[cidx]&0xff) << 24) | ((rdata[cidx+1] & 0xff)<<16) |
			((rdata[cidx+2]&0xff) << 8) | (rdata[cidx+3] & 0xff);
		cidx += 4;

		retry = ((rdata[cidx]&0xff) << 24) | ((rdata[cidx+1] & 0xff)<<16) |
			((rdata[cidx+2]&0xff) << 8) | (rdata[cidx+3] & 0xff);
		cidx += 4;

		expire = ((rdata[cidx]&0xff) << 24) | ((rdata[cidx+1] & 0xff)<<16) |
			((rdata[cidx+2]&0xff) << 8) | (rdata[cidx+3] & 0xff);
		cidx += 4;

		minimum = ((rdata[cidx]&0xff) << 24) | ((rdata[cidx+1] & 0xff)<<16) |
			((rdata[cidx+2]&0xff) << 8) | (rdata[cidx+3] & 0xff);
		cidx += 4;

		dlen = cidx - fidx;
//		System.out.println("cidx: "+cidx);
	}
}
