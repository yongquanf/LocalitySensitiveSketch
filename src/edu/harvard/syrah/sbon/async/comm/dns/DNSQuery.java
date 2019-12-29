package edu.harvard.syrah.sbon.async.comm.dns;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Vector;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.prp.PUtil;
import edu.harvard.syrah.sbon.async.CBResult;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;
import edu.harvard.syrah.sbon.async.comm.*;

/**
 * This class utilizes Question objects and a DNS server name/address (or the
 * local machine if none specified) to perform DNS lookups. This class is
 * typically used as follows.
 * 
 * <pre>
 *  
 *    // Create the query we need to run
 *    Query q = new Query( new Question( &quot;some.name.com&quot;, 
 *  		Question.TYPE_MX, Question.CLASS_IN ) );
 *    ... sometime later...
 *    // Run the query now to get the results.
 *    q.runQuery();
 *    // Run the query with another DNS server address...
 *    q.runQuery( &quot;ns.someplace.com&quot; );
 *  
 *    Then to process the results, you need to look at what came back.  We did an MX
 *    query above, so look for MX results.
 *  
 *  	RR[]ans = q.getAnswers();
 *  	for( int i = 0; i &lt; ans.length; ++i ) {
 *  		// Pretty print the result.
 *  		System.out.println( ans[i] );
 *  		if( ans[i] instanceof MXRR ) {
 *  			String exchgr = ((MXRR)ans[i]).getExchanger();
 *  			... do other stuff, considering priority etc
 *  		}
 *  	}
 *   
 * </pre>
 * 
 * Modifications by Peter Pietzuch <prp@eecs.harvard.edu> to make it
 * asynchronous.
 * 
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
 * @author Peter Pietzuch - prp@eecs.harvard.edu
 * 
 * TODO
 *   - use transactions IDs to drop unknown DNS responses
 */

public class DNSQuery {
	protected static final Log log = new Log(DNSQuery.class);

	private static final long DNS_TIMEOUT = 30000;
	
	private static int queryCounter = 1;
	
	private final int queryId;
	
	protected byte data[];

	private byte qdata[];

	private int opcode = 1;

	@SuppressWarnings("unchecked")
	private Vector l = new Vector();

	protected Vector<RR> answers;
	private Vector<RR> nsAnswers;
	private Vector<RR> extraAnswers;

	private static final boolean DEBUG = false;
	
	/**
	 * Constructs a query with 1 or more Questions.
	 * 
	 * The RFC for DNS query format specifies the following structure
	 * 
	 * <pre>
	 *  
	 *  	 4.1.1. Header section format
	 *  	 
	 *  	 The header contains the following fields:
	 *  	 
	 *  	 1  1  1  1  1  1
	 *  	 0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5
	 *  	 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	 *  	 |                      ID                       |
	 *  	 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	 *  	 |QR|   Opcode  |AA|TC|RD|RA|   Z    |   RCODE   |
	 *  	 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	 *  	 |                    QDCOUNT                    |
	 *  	 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	 *  	 |                    ANCOUNT                    |
	 *  	 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	 *  	 |                    NSCOUNT                    |
	 *  	 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	 *  	 |                    ARCOUNT                    |
	 *  	 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	 *  	 
	 *  	 
	 * </pre>
	 * 
	 * @see #runQuery()
	 * @see #runQuery(String)
	 */
	public DNSQuery(DNSQuestion req) {
		queryId = queryCounter++;
		if (queryCounter >= 255)
			queryCounter = 0;
		setQuestion(req);
	}

	public void setQuestion(DNSQuestion req) {
		opcode = 0;		// Query
		int idx = 12;	// Base Header Length
		int len = idx;
		len += req.length();
		qdata = new byte[len];
		qdata[0] = (byte)(queryId >> 8);
		qdata[1] = (byte)(queryId & 0xff);
		int val =
			(0 << 15) |		// QR
			(1 << 8) |        // Recursion Desired
			(opcode << 11) |	// Opcode
			0;
		qdata[2] = (byte)(val >> 8);
		qdata[3] = (byte)(val & 0xff);
		qdata[4] = (byte)(1 >> 8);
		qdata[5] = (byte)(1 & 0xff);
		System.arraycopy( req.getData(), 0, qdata, idx, req.length() );	
	}

	/**
	 * Constructs a query with 1 or more Questions. The RFC for DNS query format
	 * specifies the following structure
	 * 
	 * <pre>
	 *  
	 *  	 4.1.1. Header section format
	 *  	 
	 *  	 The header contains the following fields:
	 *  	 
	 *  	 1  1  1  1  1  1
	 *  	 0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5
	 *  	 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	 *  	 |                      ID                       |
	 *  	 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	 *  	 |QR|   Opcode  |AA|TC|RD|RA|   Z    |   RCODE   |
	 *  	 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	 *  	 |                    QDCOUNT                    |
	 *  	 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	 *  	 |                    ANCOUNT                    |
	 *  	 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	 *  	 |                    NSCOUNT                    |
	 *  	 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	 *  	 |                    ARCOUNT                    |
	 *  	 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	 *  	 
	 *  	 
	 * </pre>
	 * 
	 * @see #runQuery()
	 * @see #runQuery(String)
	 */
	public DNSQuery(DNSQuestion req[]) {
		queryId = queryCounter++;
		if (queryCounter >= 512)
			queryCounter = 1;
		setQuestions(req);
	}

	/**
	 * A public way to change the set of questions that the query will ask. This
	 * allows Query objects to be reused.
	 */
	public void setQuestions(DNSQuestion req[]) {
		opcode = 0; // Query
		int len = 12;  // Base Header Length
		for( int i = 0; i < req.length; ++i ) {
			len += req[i].length();
		}
		// Get a new buffer
		qdata = new byte[len];
		qdata[0] = (byte)(queryId >> 8);
		qdata[1] = (byte)(queryId & 0xff);
		int val =
			(0 << 15) |		// QR
			(1 << 8) |        // Recursion Desired
			(opcode << 11) |	// Opcode
			0;
		qdata[2] = (byte)(val >> 8);
		qdata[3] = (byte)(val & 0xff);
		qdata[4] = (byte)(req.length >> 8);
		qdata[5] = (byte)(req.length & 0xff);
		int idx = 12;
		for( int i = 0; i < req.length; ++i ) {
			len = req[i].length();
			System.arraycopy( req[i].getData(), 0, qdata, idx, len );
			idx += len;
		}		
	}
	
	public int getQueryId() {
		return queryId;
	}

	/**
	 * Get the formatted query packet.
	 */
	public byte[] getQueryData() {
		return qdata;
	}

	/**
	 * Get the formatted result packet.
	 */
	public byte[] getResultData() throws IllegalStateException {
		if (data == null)
			throw new IllegalStateException("runQuery() has not been called yet");
		return data;
	}

	/**
	 * Returns the answer records only
	 */
	public RR[] getAnswers() {
		RR[] rr = new RR[answers.size()];
		answers.copyInto(rr);
		return rr;
	}

	/**
	 * Returns the Name Server entries only
	 */
	public RR[] getNameServerAnswers() {
		RR[] rr = null;
		if (nsAnswers != null) {
			rr = new RR[nsAnswers.size()];
			nsAnswers.copyInto(rr);
		}
		return rr;
	}

	/**
	 * Returns the additional records such as A records for NS or MX queried data
	 */
	public RR[] getAdditionalAnswers() {
		RR[] rr = null;
		if (extraAnswers != null) {
			rr = new RR[extraAnswers.size()];
			extraAnswers.copyInto(rr);
		}
		return rr;
	}

	/**
	 * Runs the query that was created with the Query construct and a set of 1 or
	 * more Question objects. Uses the passed host name as the name server to
	 * contact.
	 * 
	 * @param ns
	 *          the name server to contact.
	 * 
	 * @see #Query(DNSQuestion)
	 * @see #getAnswers()
	 * @see #getNameServerAnswers()
	 * @see #getAdditionalAnswers
	 * @see #runQuery()
	 */
	public void runQuery(UDPCommIF udpComm, AddressIF dnsServer, final CB1<DNSQuery> cbQuery) {

		final int queryId = getQueryId();
		
		byte[] b = getQueryData();
		if (DEBUG) {
			for (int i = 0; i < b.length; ++i) {
				System.out.print(" ");
				System.out.print("[" + i + "]" + ((b[i] >= ' ' && b[i] <= 127) ? "(" + (char) b[i] + ")" : "(?)0x")
					+ Long.toHexString(b[i] & 0xff));
				if (((i + 1) % 2) == 0)
					System.out.println("");
			}
		}
		int len = b.length;
		final int	wlen = len;
			
		log.debug("queryId=" + queryId + " wlen=" + wlen);

//		int proper = ((b[0] << 8) & 0xff) | (b[1] & 0xff);
//		log.debug("proper=" + proper);
		
		ByteBuffer outPacket = ByteBuffer.allocate(len);

		outPacket.put(b);
		log.debug("outPacket=" + outPacket + " dnsServer=" + dnsServer);
		udpComm.sendPacket(outPacket, dnsServer, new UDPCommCB(DNS_TIMEOUT) {
			protected void cb(CBResult result, ByteBuffer inPacket, AddressIF remoteAddr, Long nanoTS, CB1<Boolean> cbHandled) {
				switch (result.state) {
					case OK: {
						log.debug("Response packet from DNS. inPacket.remaining=" + inPacket.remaining() 
							+ " inPacket.limit=" + inPacket.limit());
						//log.debug("inPacket=" + POut.print(inPacket));
						
						try {							
							InputStream is = PUtil.createInputStream(inPacket);
							//log.debug("Reading response...");
																					
							int len = inPacket.remaining();
							
							//log.debug("len=" + len);
							
							data = new byte[len];

							//log.debug("Received DNS response with responseId=" + responseId + " for queryId=" + queryId + " wlen=" + wlen);
							
							int off = 0;
							int rlen = 0;
							do {
								rlen += is.read(data, off, len - off);
							} while(rlen < len);
								
							is.close();
							
							int responseId = (data[0] & 0xff) << 8;
							responseId |= (data[1] & 0xff);
							log.debug("responseId=" + responseId);
							
							if (queryId != responseId) {
								log.warn("DNS query/response mismatch. this.queryId=" + queryId + " but responseId=" + responseId);
								//log.debug("data[0]=" + data[0] + " data[1]=" + data[1]);
								cbHandled.call(CBResult.OK(), true);
								return;
							}
							
							int rcnt = ((data[6] << 8) & 0xff) | (data[7] & 0xff);
							int nscnt = ((data[8] << 8) & 0xff) | (data[9] & 0xff);
							int arcnt = ((data[10] << 8) & 0xff) | (data[11] & 0xff);
																																																						
//							log.debug("queryID=" + transactionID);
//							
//							int flags = ((data[2] << 8) & 0xff) | (data[3] & 0xff);
//							log.debug("flags=" + flags);
							
							//int questions = ((data[4] << 8) & 0xff) | (data[5] & 0xff);
												
							byte[] b = data;
							
							//log.debug("DNS response: len=" + len + " questions=" + questions + " rcnt=" + rcnt + " nscnt=" 
							//	+ nscnt + " arcnt=" + arcnt);

//							if (DEBUG) {
//								for (int i = 0; i < b.length; ++i) {
//									System.out.print(" ");
//									System.out.print("[" + i + "]" + ((b[i] >= ' ' && b[i] <= 127) ? "(" + (char) b[i] + ")" : "(?)0x")
//										+ Long.toHexString(b[i] & 0xff));
//									if (((i + 1) % 2) == 0)
//										System.out.println("");
//								}
//								System.out.println("");
//							}

							//log.debug("wlen=" + wlen);
							
							answers = new Vector<RR>(rcnt);
							int cidx = wlen;
							
							// Process the answer section.
							//log.debug("Processing answer section...");
							for( int i = 0; i < rcnt; i++ ) {
								int rtype = RR.getRRType(data, cidx);
								RR rr = processRR(rtype, data, cidx);
								cidx += rr.dataLength();
								answers.addElement(rr);
							}

							// Process the name server section.
							//log.debug("Processing name server section...");
							nsAnswers = new Vector<RR>(nscnt);
							for( int i = 0; i < nscnt; ++i ) {
								StringBuffer buf = new StringBuffer();

								int rtype = RR.getRRType( data, cidx );
								RR rr = processRR( rtype, data, cidx );
								cidx += rr.dataLength();
								nsAnswers.addElement( rr );
							}


							// Process the extra data section.
							//log.debug("Processing extra data section...");
							extraAnswers = new Vector<RR>(arcnt);
							for( int i = 0; i < arcnt; ++i ) {
								StringBuffer buf = new StringBuffer();
								int rtype = RR.getRRType( data, cidx );
								RR rr = processRR( rtype, data, cidx );
								cidx += rr.dataLength();
								extraAnswers.addElement( rr );
							}							
							
						} catch (Exception e) {
							// We had an error
							//e.printStackTrace();
							log.warn("DNS error: " + e + " queryId=" + queryId);							
							cbHandled.call(CBResult.OK(), true);
							cbQuery.call(CBResult.ERROR(e.toString()), null);
							return;
						}

						cbHandled.call(CBResult.OK(), true);
						
						// Everthing is fine.
						//log.debug("Calling cbQuery=" + cbQuery + " cbQuery.isCanceled()=" + cbQuery.isCanceled());
						cbQuery.call(CBResult.OK(), DNSQuery.this);

						break;
					}
					case TIMEOUT: {
						log.debug("UDP timeout: " + result.toString());
						cbQuery.call(result, null);
						break;						
					}
					case ERROR: {
						log.error("UDP error: " + result.toString());
						break;												
					}
				}
			}
		});		
	}

	/**
	 * Process replies by creating new RR objects that can hold the associated
	 * data items, If you want to process a new type of record, subclass Query,
	 * and override this method to handle the record. You can still delegate to
	 * this method for the standard RRs.
	 */
	protected RR processRR(int rtype, byte data[], int cidx) throws IOException {
		RR rr;
		if (rtype == DNSQuestion.TYPE_A) { // A Record
			rr = new ARR(data, cidx);
		} else
			if (rtype == DNSQuestion.TYPE_NS) { // NS Record
				rr = new NSRR(data, cidx);
			} else
				if (rtype == DNSQuestion.TYPE_PTR) { // PTR Record
					rr = new PTRRR(data, cidx);
				} else
					if (rtype == DNSQuestion.TYPE_MX) { // MX Record
						rr = new MXRR(data, cidx);
					} else
						if (rtype == DNSQuestion.TYPE_MG) { // MG Record
							rr = new MGRR(data, cidx);
						} else
							if (rtype == DNSQuestion.TYPE_TXT) { // TXT Record
								rr = new TXTRR(data, cidx);
							} else
								if (rtype == DNSQuestion.TYPE_MINFO) { // MINFO Record
									rr = new MINFORR(data, cidx);
								} else
									if (rtype == DNSQuestion.TYPE_HINFO) { // HINFO Record
										rr = new HINFORR(data, cidx);
									} else
										if (rtype == DNSQuestion.TYPE_MR) { // MR Record
											rr = new MRRR(data, cidx);
										} else
											if (rtype == DNSQuestion.TYPE_WKS) { // WKS Record
												rr = new WKSRR(data, cidx);
											} else
												if (rtype == DNSQuestion.TYPE_MB) { // MB Record
													rr = new MBRR(data, cidx);
												} else
													if (rtype == DNSQuestion.TYPE_MD) { // MD Record
														rr = new MDRR(data, cidx);
													} else
														if (rtype == DNSQuestion.TYPE_MF) { // MF Record
															rr = new MFRR(data, cidx);
														} else
															if (rtype == DNSQuestion.TYPE_NULL) { // NULL Record
																rr = new NULLRR(data, cidx);
															} else
																if (rtype == DNSQuestion.TYPE_SOA) { // SOA Record
																	rr = new SOARR(data, cidx);
																} else
																	if (rtype == DNSQuestion.TYPE_CNAME) { // CNAME Record
																		rr = new CNAMERR(data, cidx);																	
																	} else 
																		if (rtype == DNSQuestion.TYPE_AAAA) { // AAAA Record
																			rr = new AAAARR(data, cidx);
																		} else {
																			throw new IOException("unsupported answer, type=" + DNSQuestion.toType(rtype));
																		}
		return rr;
	}
	
	/**
	 * A helper method that will run a query for you and print out the results in
	 * a manner similar to what dig(1) or other DNS query programs might show the
	 * user.
	 */
	public static void printQuery(DNSQuery q) {

		// Get the answer objects
		RR[] ans = q.getAnswers();
		System.out.println("answers: " + ans.length);
		for (int i = 0; i < ans.length; ++i)
			System.out.println(ans[i]);

		// Get the nameserver entries if any
		RR[] nsans = q.getNameServerAnswers();
		if (nsans != null) {
			System.out.println("ns answers: " + nsans.length);
			for (int i = 0; i < nsans.length; ++i)
				System.out.println(nsans[i]);
		}

		// Get any extra entries returned
		RR[] exans = q.getAdditionalAnswers();
		if (exans != null) {		
			System.out.println("extra data answers: " + exans.length);
			for (int i = 0; i < exans.length; ++i)
				System.out.println(exans[i]);
		}
	}

}