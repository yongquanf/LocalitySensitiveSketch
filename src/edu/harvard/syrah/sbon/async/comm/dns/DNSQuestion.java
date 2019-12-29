package edu.harvard.syrah.sbon.async.comm.dns;

/**
 *  This class wraps the 3 pieces of information needed to execute a DNS query.
 *  The string value of concern (a domain structured name) and then the type 
 *  and class of query.
 <p>
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
public class DNSQuestion {
	protected byte data[];
	protected int len;
	protected String name;
	protected int qtype;
	protected int qclass;

	/** The Internet class */
	public static final int CLASS_IN = 1;
	/** The "A" record/query type. */
	public static final int TYPE_A = 1;
	/** The "NS" record/query type. */
	public static final int TYPE_NS = 2;
	/** The "MD" record/query type. */
	public static final int TYPE_MD = 3;
	/** The "MF" record/query type. */
	public static final int TYPE_MF = 4;
	/** The "CNAME" record/query type. */
	public static final int TYPE_CNAME = 5;
	/** The "SOA" record/query type. */
	public static final int TYPE_SOA = 6;
	/** The "MB" record/query type. */
	public static final int TYPE_MB = 7;
	/** The "MG" record/query type. */
	public static final int TYPE_MG = 8;
	/** The "MR" record/query type. */
	public static final int TYPE_MR = 9;
	/** The "NULL" record/query type. */
	public static final int TYPE_NULL = 10;
	/** The "WKS" record/query type. */
	public static final int TYPE_WKS = 11;
	/** The "PTR" record/query type. */
	public static final int TYPE_PTR = 12;
	/** The "HINFO" record/query type. */
	public static final int TYPE_HINFO = 13;
	/** The "MINFO" record/query type. */
	public static final int TYPE_MINFO = 14;
	/** The "MX" record/query type. */
	public static final int TYPE_MX = 15;
	/** The "TXT" record/query type. */
	public static final int TYPE_TXT = 16;
	/** The "AAAA" record/query type. */
	public static final int TYPE_AAAA = 28;

	/**
	 *  Formats a time in seconds into a string describing the amount of
	 *  time indicated.
	 *
	 *  e.g.  601 becomes  "10m1s"  for 10 minutes 1 second.
	 */
	public static String toTime( int ttl ) {
		String buf = "";
		while( ttl > 0 ) {
			int v = 0;
			if( ttl < 60 ) {
				buf += ttl+"s";
				ttl = 0;
			} else if( ttl < 3600 ) {
				v = ttl / (60);
				buf += v+"m";
				ttl = ttl % 60;
			} else if( ttl < 3600 * 24 ) {
				v = ttl / 3600;
				buf += v+"h";
				ttl = ttl % 3600;
			} else if( ttl < (3600 * 24 * 7) ) {
				v = ttl / (3600 * 24);
				buf += v+"d";
				ttl = ttl % (3600 * 24);
			} else {
				v = ttl / (3600 * 24 * 7);
				buf += v+"w";
				ttl = ttl % (3600*24 * 7);
			}
		}
		return buf;
	}

	/** Formats the question data to a descriptive string */
	public String toString() {
		return toType(qtype)+"["+toClass(qclass)+"] for "+name;
	}

	/**
	 *  Returns a string describing the indicated CLASS_* value.
	 *  currently only IN is supported
	 */
	public static String toClass( int cls ) {
		String buf = ""+cls;
		if( cls == CLASS_IN )
			buf = "IN";
		return buf;
	}

	/**
	 *  Translates one of the TYPE_* values into a string
	 *  that is the part after the TYPE_ part of the variable
	 *  name.
	 */
	public static String toType( int type ) {
		return RR.toType( type );
	}

	/**
	 *  Used to change the name to reuse this Question for another query.
	 *
	 *  @param name the name string to ask a question about.
	 */
	public void setName( String name ) {
		this.name = name;
	}

	/**
	 *  Used to change the type to reuse this Question for another query.
	 *  @param type one of the TYPE_* values.
	 */
	public void setType( int type ) {
		this.qtype = type;
	}

	/** 
	 *  Used to change the class to reuse this Question for another query.
	 *  @param cls one of the CLASS_* values
	 */
	public void setClass( int cls ) {
		this.qclass = cls;
	}

	/**
	 *  Constructs a new Question.  The query data is formatted
	 *  from this constructor and placed into the <code>data</code>
	 *  buffer.
	 *
	 *  @param name the domain string to query about
	 *  @param qtype one of the TYPE_* query types.
	 *  @param qclass one of the CLASS_* values.
	 */
	public DNSQuestion( String name, int qtype, int qclass ) {
		int nlen = RR.nameLength(name);
		this.name = name;
		this.qtype = qtype;
		this.qclass = qclass;
		data = new byte[nlen + 4];
		RR.copyInName( data, 0, name );
		data[ nlen ] = (byte)(qtype>>8);
		data[ nlen+1 ] = (byte)(qtype & 0xff);
		data[ nlen+2 ] = (byte)(qclass>>8);
		data[ nlen+3 ] = (byte)(qclass & 0xff);
		len = nlen+4;
	}

	/**
	 *  Returns the total length in bytes of the query data buffer
	 */
	public int length() {
		return len;
	}

	/** Returns the data buffer for the query */
	public byte[] getData() {
		return data;
	}
}
