/*
 * @author Last modified by $Author: ledlie $
 * @version $Revision: 1.4 $ on $Date: 2009/03/27 17:39:58 $
 * @since Jul 13, 2005
 */
package edu.harvard.syrah.sbon.async.comm.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.InvalidMarkException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.prp.NetUtil;
import edu.harvard.syrah.prp.POut;
import edu.harvard.syrah.prp.PUtil;
import edu.harvard.syrah.sbon.async.CBResult;
import edu.harvard.syrah.sbon.async.EL;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB2;
import edu.harvard.syrah.sbon.async.comm.AddressFactory;
import edu.harvard.syrah.sbon.async.comm.AddressIF;
import edu.harvard.syrah.sbon.async.comm.Comm;
import edu.harvard.syrah.sbon.async.comm.NetAddress;
import edu.harvard.syrah.sbon.async.comm.TCPComm;

public class HTTPComm extends TCPComm implements HTTPCommIF {
	protected static final Log log = new Log(HTTPComm.class);

	public static final int HTTP_OK_CODE = 200;
	public static final int HTTP_NOT_FOUND_CODE = 404;

	private static final long HTTP_KEEPALIVE_TIMEOUT = 30000;

	// Timeout after 5 mins
	private static final long HTTP_HANDLER_TIMEOUT = 300000;

	private static final String HTTP_IMPL_VERSION = "AsyncJ HTTP 0.1.5";
	private static final String HTTP_11 = "HTTP/1.1";
	protected static final byte[] HTTP_OK = NetUtil.toHTTPBytes(" 200 OK\r\n");
	protected static final byte[] HTTP_SERVER = NetUtil.toHTTPBytes("Server: " + HTTP_IMPL_VERSION
			+ " \r\n");
	protected static final byte[] HTTP_CONN_KEEP = NetUtil.toHTTPBytes("Connection: Keep-Alive\r\n");
	protected static final byte[] HTTP_CONN_CLOSE = NetUtil.toHTTPBytes("Connection: close\r\n");
	protected static final byte[] HTTP_CON_TYPE = NetUtil.toHTTPBytes("Content-Type: ");
	protected static final byte[] HTTP_CON_LENGTH = NetUtil.toHTTPBytes("Content-Length: ");
	protected static final byte[] HTTP_POST = NetUtil.toHTTPBytes("POST ");
	protected static final byte[] HTTP_GET = NetUtil.toHTTPBytes("GET ");
	protected static final byte[] HTTP_VERSION = NetUtil.toHTTPBytes("HTTP/1.1");
	protected static final byte[] HTTP_USER_AGENT = NetUtil.toHTTPBytes("User-Agent: "
			+ HTTP_IMPL_VERSION + " \r\n");
	protected static final byte[] HTTP_HOST = NetUtil.toHTTPBytes("Host: ");
	private static final String HTTP_ENCODING = "US-ASCII";

	protected boolean serverKeepAlive;

	private Map<String, HTTPCallbackHandler> httpRequestHandlers = new HashMap<String, HTTPCallbackHandler>(); 

	public HTTPComm() {
		super();
	}

	public void initServer(AddressIF bindAddress, boolean serverKeepAlive, CB0 cbInit) {		
		super.initServer(bindAddress, cbInit);
		this.serverKeepAlive = serverKeepAlive;
	}

	public void sendHTTPRequest(String urlString, final String httpRequestString,
			final boolean keepAlive, final HTTPCB cbHTTPResponse) {
		sendHTTPRequest(urlString, httpRequestString, keepAlive, false, cbHTTPResponse);
	}

	public void sendHTTPStreamRequest(String urlString, final String httpRequestString,
			final HTTPCB cbHTTPResponse) {
		sendHTTPRequest(urlString, httpRequestString, true, true, cbHTTPResponse);
	}

	private void sendHTTPRequest(String urlString, final String httpRequestString,
			final boolean keepAlive, final boolean streamSemantics, final HTTPCB cbHTTPResponse) {

		if (httpRequestString.length() > MAX_BUFFER_SIZE) {
			cbHTTPResponse.call(CBResult.ERROR("HTTPRequest too large: httpRequestString.length="
					+ httpRequestString.length() + " MAX_BUFFER_SIZE=" + MAX_BUFFER_SIZE), 0, null, null);
		}

		URL url = null;
		try {
			url = new URL(urlString);

			// Add default path if missing
			if (url.getPath().length() == 0)
				url = new URL(urlString + "/");
		} catch (MalformedURLException e) {
			cbHTTPResponse.call(CBResult.ERROR(e.toString()), 0, null, null);
			return;
		}

		final URL finalURL = url;

		AddressFactory.createResolved(url.getHost(), url.getPort() != -1 ? url.getPort() : 80,
				new CB1<AddressIF>() {

					protected void cb(CBResult result, AddressIF destAddr) {
						switch (result.state) {
							case OK: {
								log.debug("Creating new http connection to destAddr=" + destAddr);

								Comm.ConnectConnHandler connectConnHandler = HTTPComm.this.createConnection(
										destAddr, keepAlive && (!streamSemantics), streamSemantics ? 0
												: HTTP_KEEPALIVE_TIMEOUT, new CB0() {

											protected void cb(CBResult result) {
												switch (result.state) {
													case OK: {
														/* ignore */
														break;
													}
													case TIMEOUT:
													case ERROR: {
														cbHTTPResponse.call(result, 0, null, null);
														break;
													}
												}
											}
										});

								HTTPRequest httpRequest = new HTTPRequest(destAddr, finalURL, null,
										httpRequestString, keepAlive, streamSemantics, cbHTTPResponse);

								if (connectConnHandler != null) {
									((ConnectConnHandler) connectConnHandler).addHTTPRequest(httpRequest);
								} else {
									Comm.WriteConnHandler writeConnHandler = destConnectionPool.get(destAddr);
									assert writeConnHandler != null;
									((WriteRequestConnHandler) writeConnHandler).addHTTPRequest(httpRequest);
									((WriteRequestConnHandler) writeConnHandler).register();
								}
								// log.debug("pendingConnectionPool=" +
								// Util.toString(pendingConnectionPool.keySet()));
								break;
							}
							case TIMEOUT: {
								cbHTTPResponse.call(result, 0, null, null);
								break;
							}
							case ERROR: {
								cbHTTPResponse
										.call(CBResult.ERROR("Unknown host. addr=" + destAddr), 0, null, null);
								break;
							}
						}
					}
				});
	}

	public void checkHTTP(String url, boolean keepAlive, HTTPCB cbHTTPResponse) {
		sendHTTPRequest(url, "HEAD", keepAlive, false, cbHTTPResponse);
	}

	public void registerHandler(String path, HTTPCallbackHandler httpRequestHandler) {
		assert httpRequestHandler != null;
		httpRequestHandlers.put(path, httpRequestHandler);
	}

	public void deregisterXMLRPCHandler(String path) {
		httpRequestHandlers.remove(path);
	}
	
	private Map<String,String> getParameters(String uri) {
		log.debug ("getParameters uri="+uri);
		int qmark = uri.indexOf("?");
		if (qmark > 0 && uri.length() > qmark+1) {
			String params = uri.substring(qmark+1, uri.length());
			log.debug("params="+params);
			String[] kvs = params.split("&");
			Map<String,String> map = new HashMap<String,String> ();
			log.debug ("kvs length="+kvs.length);
			for (int i = 0; i < kvs.length; i++) {
				String[] kv = kvs[i].split("=");
				log.debug ("kv length="+kv.length);
				if (kv.length == 2) {
					//String key = NetUtil.fromHTTPBytes(kv[0]);
					map.put (kv[0],kv[1]);
					log.debug ("key="+kv[0]+" value="+kv[1]);
				}
			}
			return map;
			
		} else {
			return null;
		}
	}
	
	/*
	private String getMessageBody (String httpRequest, int dataStartMarker) {
		// drop the first new line
		if (dataStartMarker == 0) {
			return null;
		}
		return httpRequest.substring(dataStartMarker, httpRequest.length());
	}
	*/
	
	private HTTPCallbackHandler getRequestHandler(String path) {
		// Remove trailing slash for matching purposes 
		if (path.endsWith("/"))
			path = path.substring(0, path.length() - 1);
				
		int qmark = path.indexOf("?");
		if (qmark > 0) {
			path = path.substring(0, qmark);
		}
		HTTPCallbackHandler handler = httpRequestHandlers.get(path);
		return handler;
	}

	protected AcceptConnHandler getAcceptConnHandler(NetAddress localAddress, CB0 cbHandler) {
		return new AcceptConnHandler(localAddress, cbHandler);
	}

	protected ConnectConnHandler getConnectConnHandler(NetAddress remoteAddress, long timeout,
			CB0 cbHandler) {
		return new ConnectConnHandler(remoteAddress, timeout, cbHandler);
	}

	protected ReadRequestConnHandler getReadRequestConnHandler(SelectableChannel channel,
			NetAddress remoteAddr) {
		ReadRequestConnHandler readRequestConnHandler = (ReadRequestConnHandler) EL.get()
				.getCommCB(channel, SelectionKey.OP_READ);
		if (readRequestConnHandler == null) {
			readRequestConnHandler = new ReadRequestConnHandler(channel, remoteAddr);
		}
		return readRequestConnHandler;
	}

	protected WriteResponseConnHandler getWriteResponseConnHandler(SelectableChannel channel,
			NetAddress remoteAddr) {
		WriteResponseConnHandler writeResponseConnHandler = (WriteResponseConnHandler) EL.get()
				.getCommCB(channel, SelectionKey.OP_WRITE);
		if (writeResponseConnHandler == null) {
			writeResponseConnHandler = new WriteResponseConnHandler(channel, remoteAddr);
		}
		return writeResponseConnHandler;
	}

	protected ReadResponseConnHandler getReadResponseConnHandler(SelectableChannel channel,
			NetAddress remoteAddr) {
		ReadResponseConnHandler readResponseConnHandler = (ReadResponseConnHandler) EL.get()
				.getCommCB(channel, SelectionKey.OP_READ);
		if (readResponseConnHandler == null) {
			readResponseConnHandler = new ReadResponseConnHandler(channel, remoteAddr);
		}
		return readResponseConnHandler;
	}

	protected WriteRequestConnHandler getWriteRequestConnHandler(SelectableChannel channel,
			NetAddress remoteAddr) {
		WriteRequestConnHandler writeRequestConnHandler = (WriteRequestConnHandler) EL.get()
				.getCommCB(channel, SelectionKey.OP_WRITE);
		if (writeRequestConnHandler == null) {
			writeRequestConnHandler = new WriteRequestConnHandler(channel, remoteAddr);
			destConnectionPool.put(remoteAddr, writeRequestConnHandler);
			//this.destConnectionTried.remove(remoteAddr);
		}
		return writeRequestConnHandler;
	}

	/*
	 * Callback to accept a new connection
	 */
	class AcceptConnHandler extends TCPComm.AcceptConnHandler {
		AcceptConnHandler(AddressIF localAddress, CB0 cbHandler) {
			super(localAddress, cbHandler);
		}

		public Boolean cb(CBResult result, SelectionKey key) {
			super.cb(result, key);

			if (key.isValid() && key.isAcceptable()) {
				ReadRequestConnHandler readRequestConnHandler = getReadRequestConnHandler(channel, null);
				readRequestConnHandler.register();
			}
			return true;
		}
	}

	/*
	 * Callback for connecting a new connection
	 */
	class ConnectConnHandler extends TCPComm.ConnectConnHandler {
		private final Log log = new Log(ConnectConnHandler.class);

		private List<HTTPRequest> httpRequestList = new LinkedList<HTTPRequest>();

		ConnectConnHandler(NetAddress destAddr, long timeout, CB0 handlerCB) {
			super(destAddr, timeout, handlerCB);
		}

		public Boolean cb(CBResult result, SelectionKey key) {
			super.cb(result, key);

			if (key.isValid() && key.isConnectable()) {
				ReadResponseConnHandler readResponseConnHandler = getReadResponseConnHandler(channel,
						remoteAddr);

				WriteRequestConnHandler writeRequestConnHandler = getWriteRequestConnHandler(channel,
						remoteAddr);

				for (Iterator<HTTPRequest> httpRequestIt = httpRequestList.iterator(); httpRequestIt
						.hasNext();) {
					HTTPRequest httpRequest = httpRequestIt.next();
					writeRequestConnHandler.addHTTPRequest(httpRequest);
					httpRequestIt.remove();
				}
				writeRequestConnHandler.register();

				// Return connection callbacks
				cbHandler.call(CBResult.OK());

			}
			return true;
		}

		protected void addHTTPRequest(HTTPRequest httpRequest) {
			httpRequestList.add(httpRequest);
		}
	}

	/*
	 * Callback to write request data from an existing connection
	 */
	class WriteRequestConnHandler extends TCPComm.WriteConnHandler {
		private final Log log = new Log(WriteRequestConnHandler.class);

		private boolean keepAlive = false;
		private List<HTTPRequest> httpRequestList = new LinkedList<HTTPRequest>();

		WriteRequestConnHandler(SelectableChannel channel, NetAddress remoteAddr) {
			super(channel, remoteAddr);
		}

		public Boolean cb(CBResult result, SelectionKey key) {
			super.cb(result, key);
			SocketChannel socketChannel = (SocketChannel) channel;

			if (key.isWritable()) {
				log.debug("Writing to socket...");

				if ((buffer.position() == 0) && (!httpRequestList.isEmpty())) {
					handleNextRequest();
				}

				buffer.flip();

				int count = 0;
				try {
					count = socketChannel.write(buffer);
				} catch (IOException e) {
					log.warn("Error writing to socket: " + e);
					closeConnection(remoteAddr, socketChannel);
					return true;
				}

				int limit = buffer.limit();
				// log.debug("limit=" + limit);
				buffer.compact();
				buffer.position(limit - count);

				if (buffer.position() == 0) {
					log.debug("Write buffer is empty.");
					ReadResponseConnHandler readResponseConnHandler = (ReadResponseConnHandler) EL
							.get().getCommCB(channel, SelectionKey.OP_READ);
					readResponseConnHandler.register();
					deregister();
				}

			} else {
				log.error("We received a writable callback for a key that is not writable. Bug?");
			}
			return true;
		}

		protected void addHTTPRequest(HTTPRequest httpRequest) {
			httpRequestList.add(httpRequest);
		}

		private void writeHTTPRequest(URL url, boolean keepAlive, String contentType, String httpRequest) {
			this.keepAlive = keepAlive;
			byte[] request = (httpRequest != null) ? NetUtil.toHTTPBytes(httpRequest) : new byte[] {};

			// Only send a POST if we have data
			if (httpRequest != null) {
				buffer.put(HTTP_POST);
			} else {
				buffer.put(HTTP_GET);
			}
			buffer.put(NetUtil.toHTTPBytes(url.getPath() + " "));
			buffer.put(HTTP_VERSION);
			buffer.put(NetUtil.HTTP_NEWLINE);
			buffer.put(HTTP_USER_AGENT);

			buffer.put(HTTP_HOST);
			int port = remoteAddr.getPort();
			String hostStr = remoteAddr.getHostname() + (port == 80 ? "" : ":" + port);
			buffer.put(NetUtil.toHTTPBytes(hostStr));
			buffer.put(NetUtil.HTTP_NEWLINE);

			if (keepAlive) {
				buffer.put(HTTP_CONN_KEEP);
			}
			if (contentType != null) {
				buffer.put(HTTP_CON_TYPE);
				buffer.put(NetUtil.toHTTPBytes(contentType));
				buffer.put(NetUtil.HTTP_NEWLINE);
			}
			buffer.put(HTTP_CON_LENGTH);
			buffer.put(NetUtil.toHTTPBytes(Integer.toString(request.length)));
			buffer.put(NetUtil.HTTP_NEWLINE);
			buffer.put(NetUtil.HTTP_NEWLINE);

			if (buffer.remaining() < request.length) {
				log.warn("Extending buffer=" + buffer + " response.length=" + request.length);
				buffer = PUtil.extendByteBuffer(buffer, buffer.capacity()
						+ (request.length - buffer.remaining()));
				buffer.limit(buffer.capacity());
				log.debug("new buffer=" + buffer);
			}

			buffer.put(request);

			log.debug("Method is " + (httpRequest == null ? "GET" : "POST") + " url=" + url + " hostStr="
					+ hostStr);
			// log.debug("buffer=" + Util.printChar(buffer));
		}

		private void handleNextRequest() {
			if (!httpRequestList.isEmpty()) {
				HTTPRequest httpRequest = httpRequestList.remove(0);
				this.remoteAddr = (NetAddress) httpRequest.destAddr;
				ReadResponseConnHandler readResponseConnHandler = (ReadResponseConnHandler) EL.get()
						.getCommCB(channel, SelectionKey.OP_READ);
				readResponseConnHandler.setCB(httpRequest.cb);
				readResponseConnHandler.keepAlive = httpRequest.keepAlive;
				readResponseConnHandler.streamSemantics = httpRequest.streamSemantics;
				writeHTTPRequest(httpRequest.url, httpRequest.keepAlive, httpRequest.contentType,
						httpRequest.httpRequest);
			}
		}

		public boolean hasMoreRequests() {
			return !httpRequestList.isEmpty();
		}
	}

	/*
	 * Callback to read response data from an existing connection
	 */
	class ReadResponseConnHandler extends TCPComm.ReadConnHandler {
		protected final Log log = new Log(ReadResponseConnHandler.class);

		// Size of the buffer used for reading from connections
		private static final int UNKNOWN_LENGTH = -1;
		private static final int STREAM_MARKER = 0;

		private BufferedReader br = null;
		private InputStream is = null;

		private String httpVersion;
		protected int resultCode;
		protected String httpResponse;
		private int contentLength = UNKNOWN_LENGTH;

		// Marker for the begining of the http code
		private int httpMarker = UNKNOWN_LENGTH;

		protected HTTPCB cb = null;
		private CB0 cancelledCB = null;
		protected boolean keepAlive;
		protected boolean streamSemantics;

		ReadResponseConnHandler(SelectableChannel channel, NetAddress remoteAddr) {
			super(channel, remoteAddr);
		}

		public Boolean cb(CBResult result, SelectionKey key) {
			super.cb(result, key);

			if (key.isReadable()) {
				log.debug("Reading from socket...");
				channel = key.channel();
				SocketChannel socketChannel = (SocketChannel) channel;

				log.debug("buffer=" + buffer);

				if (buffer.remaining() == 0) {
					log.debug("The read buffer is full. buffer=" + buffer + ". Extending...");
					if (buffer.capacity() * 2 < MAX_BUFFER_SIZE) {
						buffer = PUtil.extendByteBuffer(buffer, buffer.capacity() * 2);
						buffer.limit(buffer.capacity());
						log.debug("new buffer=" + buffer);
					} else {
						log.warn("Buffer larger than MAX_BUFFER_SIZE: buffer=" + buffer + " remoteAddr="
								+ remoteAddr + " channel=" + channel);
						cb.call(CBResult.ERROR("Too much data for buffer"), resultCode, httpResponse, null);
						return true;
					}
				}

				int count = 0;
				try {
					count = socketChannel.read(buffer);
				} catch (IOException e) {
					// The other party closed the connection -- handle this gracefully.
					log.debug("Error reading from socket: " + e);
					count = -1;
				}

				if (count == -1 || count == 0) {
					/*
					 * There is some confusion on what count == 0 means but we'll just
					 * treat it as EOF and close the connection.
					 */
					log.debug("Connection to remoteAddr=" + remoteAddr + " has been closed.");
					closeConnection(remoteAddr, socketChannel);
					if (cb != null)
						cb.deregisterCancelledCB(cancelledCB);

					// Make sure that we set the contentLength value for broken web
					// servers that don't provide it
					if (contentLength == UNKNOWN_LENGTH) {
						if (httpMarker != UNKNOWN_LENGTH) {
							contentLength = buffer.position() - httpMarker;

							// TODO This may not be correct but it appears that content length
							// is only less than zero for broken webservers...?
							if (contentLength < 0) {
								contentLength = 0;
								log.warn("contentLength < 0: httpMarker=" + httpMarker + " count=" + count
										+ " buffer=" + buffer + " remoteAddr=" + remoteAddr);
							}
							assert contentLength >= 0 : "contentLength=" + contentLength + " httpMarker="
									+ httpMarker + " count=" + count + " buffer=" + buffer + " remoteAddr="
									+ remoteAddr;

							log.debug("No content-length found. Setting to " + contentLength);
						} else {
							log.debug("No data received. Returning. buffer=" + buffer);
							cb.call(CBResult.ERROR("No http data received"), resultCode, httpResponse, null);
							return true;
						}
					}
				}

				buffer.flip();

				if (br == null) {
					is = PUtil.createInputStream(buffer);
					br = new BufferedReader(new InputStreamReader(is));
				}

				// log.debug("buffer.position=" + buffer.position());
				log.debug("count=" + count + " contentLength=" + contentLength);

				String line = null;
				int bytesRead = UNKNOWN_LENGTH;

				// Read the header data
				if (contentLength == UNKNOWN_LENGTH && httpMarker != STREAM_MARKER) {

					log.debug("Reading the header...");

					do {
						try {
							line = br.readLine();
						} catch (IOException e) {
							log.error("Failed reading line from buffer: " + e);
						}

						if (line != null) {
							log.debug("line='" + line + "'");
							bytesRead += line.length() + 2; // A newline is two bytes

							if (httpVersion == null) {
								StringTokenizer tokenizer = new StringTokenizer(line);
								httpVersion = tokenizer.nextToken();
								keepAlive = keepAlive && HTTP_11.equals(httpVersion);
								String resultCodeStr = null;
								try {
									resultCodeStr = tokenizer.nextToken();
									resultCode = Integer.valueOf(resultCodeStr);
								} catch (NumberFormatException e) {
									log.warn("No valid result code received: " + resultCodeStr);
									resultCode = 400;
								} catch (NoSuchElementException e) {
									log.warn("No result code received: " + resultCodeStr + " tokenizer=" + tokenizer);
									resultCode = 400;
								}

								while (tokenizer.hasMoreTokens()) {
									httpResponse = (httpResponse == null) ? tokenizer.nextToken() : httpResponse
											+ " " + tokenizer.nextToken();
								}
							}

							String lineLower = line.toLowerCase();
							if (lineLower.startsWith("content-length:")) {
								contentLength = Integer.parseInt(line.substring(15).trim());
								assert contentLength >= 0 : "contentLength=" + contentLength + " lineLower="
										+ lineLower;
								log.debug("Content-length: " + contentLength);
							}
							if (lineLower.startsWith("connection:")) {
								keepAlive = keepAlive && lineLower.indexOf("keep-alive") > -1;
							}

						}
					} while (line != null && line.length() != 0);

					if ((httpMarker == UNKNOWN_LENGTH) && (line != null) && (line.length() == 0)) {
						httpMarker = bytesRead;
						log.debug("Found end of header at httpMark=" + httpMarker);
					}
				}

				log.debug("httpMarker=" + httpMarker + " buffer=" + buffer + " line='" + line + "'");
				if (httpMarker != UNKNOWN_LENGTH
						&& (contentLength != UNKNOWN_LENGTH || (streamSemantics && httpMarker != bytesRead) || resultCode != HTTP_OK_CODE)) {

					int available = buffer.limit() - httpMarker;
					log.debug("Availabe data=" + available);

					if (available >= contentLength || streamSemantics) {

						log.debug("Got the complete http response.");
						try {
							buffer.position(httpMarker);
						} catch (IllegalArgumentException e) {
							log.error("httpMarker=" + httpMarker + " buffer=" + buffer + " contentLength="
									+ contentLength + " available=" + available + " streamSemantics="
									+ streamSemantics + " bytesRead=" + bytesRead + " line=" + line + " count="
									+ count + " :" + e);
						}

						String responseString = null;
						if (!streamSemantics) {

							responseString = new String(buffer.array(), buffer.position(), buffer.remaining());
							buffer.position(buffer.limit());
							buffer.compact();
							log.debug("compact buffer=" + buffer);
							buffer.limit(0);

						} else {

							log.debug("Starting to read the stream...");

							InputStream responseIS = PUtil.createInputStream(buffer);
							BufferedReader responseReader = new BufferedReader(new InputStreamReader(responseIS));

							try {
								String l = null;
								StringBuffer responseBuffer = new StringBuffer();
								int chunkHeaderLength = 0;
								int chunkDataLength = 0;
								int chunkMissingData = 0;

								// bytesRead = 0;
								// int chunkStartPos = 0;

								do {
									l = responseReader.readLine();
									// log.debug("Reading l.length=" + (l != null ?
									// String.valueOf(l.length()) : "null") + " l='" + l + "'");

									if (l != null) {

										bytesRead = l.length();

										if (l.matches("^[0-9a-fA-F]+;$")) {
											assert chunkMissingData == 0 : "chunkMissingData=" + chunkMissingData + " l="
													+ l;
											assert chunkDataLength == 0;
											assert chunkHeaderLength == 0;
											log.debug("chunkStart");
											chunkHeaderLength = l.length() + POut.NL.length() + 1;

											String hexLength = l.substring(0, l.length() - 1);
											chunkDataLength = Integer.valueOf(hexLength, 16);

											chunkMissingData = chunkDataLength;

											log.debug("chunkHeaderLength=" + chunkHeaderLength + " chunkDataLength="
													+ chunkDataLength + " chunkMissingData=" + chunkMissingData);
											continue;
										}

										if (chunkMissingData == 0 && l.length() == 0) {
											continue;
										}

										if (chunkMissingData != 0) {
											chunkMissingData -= l.length() + POut.NL.length();
											log.debug("chunkMissingData=" + chunkMissingData);

											if (chunkMissingData == 0) {
												chunkDataLength = 0;
												chunkHeaderLength = 0;
											}
										}

										responseBuffer.append(l + POut.NL);
										log.debug("responseBuffer.length=" + responseBuffer.length());
									}

								} while (l != null);

								log.debug("chunkDataLength=" + chunkDataLength + " chunkMissingData="
										+ chunkMissingData);
								int chunkPartialData = chunkDataLength - chunkMissingData;

								if (chunkPartialData > 0) {
									log.debug("Removing partial chunk. chunkPartialData=" + chunkPartialData);
									responseBuffer.delete(responseBuffer.length() - chunkPartialData, responseBuffer
											.length());
								}

								responseReader.close();
								responseIS.close();

								log.debug("responseBuffer.length=" + responseBuffer.length());

								responseString = responseBuffer.toString();

								if (chunkHeaderLength == 0 && bytesRead != 0) {
									log.debug("Partial header.");
									chunkHeaderLength = bytesRead;
								}

								/*
								 * TODO there seems to be a bug in this code, which causes an
								 * IllegalArgumentException
								 */

								if (chunkHeaderLength == -1)
									chunkHeaderLength = 0;

								log.debug("chunkHeaderLength=" + chunkHeaderLength + " chunkPartialData="
										+ chunkPartialData);
								try {
									buffer.position(buffer.position() - chunkHeaderLength - chunkPartialData);
									buffer.limit(buffer.position() + chunkHeaderLength + chunkPartialData);
									log.debug("before compact=" + buffer);
									buffer.compact();
									log.debug("after compact=" + buffer);
									buffer.limit(chunkHeaderLength + chunkPartialData);
								} catch (IllegalArgumentException e) {
									log.warn("buffer.contents=" + POut.print(buffer) + " responseString="
											+ responseString);
									log.error(e + " chunkHeaderLength=" + chunkHeaderLength + " chunkPartialData="
											+ chunkPartialData + " chunkDataLength=" + chunkDataLength
											+ " chunkMissingData=" + chunkMissingData + " httpMarker=" + httpMarker
											+ " buffer=" + buffer);
								}

								// log.debug("buffer.content=" + POut.print(buffer));

								httpMarker = STREAM_MARKER;

							} catch (IOException e) {
								log.error("Could not read http stream: " + e);
							}
						}

						if (!streamSemantics || !responseString.equals(POut.NL)) {
							if (!cb.isCancelled()) {
								final String finalResponseString = responseString;
								log.debug("finalResponseString.length=" + finalResponseString.length());
								log.debug("Executing application cb...");
								EL.get().registerTimerCB(new CB0() {
									protected void cb(CBResult result) {
										cb.call(CBResult.OK(), resultCode, httpResponse, finalResponseString);
										log.debug("Cb has returned");
									}
								});
							} else {
								log.debug("cb has been canceled...");
								// Assume that the connection has already been closed
								try {
									br.close();
									is.close();
								} catch (IOException e) {
									log.error("Could not close input stream: " + e);
								}
							}
						}

						if (!streamSemantics) {

							try {
								br.close();
								is.close();
							} catch (IOException e) {
								log.error("Could not close input stream: " + e);
							}

							if (channel.isOpen()) {

								WriteRequestConnHandler writeRequestConnHandler = (WriteRequestConnHandler) EL
										.get().getCommCB(channel, SelectionKey.OP_WRITE);
								assert writeRequestConnHandler != null;

								deregister();

								if (!keepAlive) {
									log.debug("No keepAlive. Closing the connection to remoteAddr=" + remoteAddr);
									closeConnection(remoteAddr, socketChannel);

									if (cb != null)
										cb.deregisterCancelledCB(cancelledCB);

								} else {
									// Reset the content lenght field
									contentLength = UNKNOWN_LENGTH;
									httpMarker = UNKNOWN_LENGTH;

									// Register the WriteRequestHandler if there are outstanding
									// requests
									if (writeRequestConnHandler.hasMoreRequests()) {
										writeRequestConnHandler.register();
									}
								}
							} else {
								log.debug("Channel has already been closed");
							}
						}
					} else {
						log.debug("Waiting for remainder of response. missing=" + (contentLength - available));
					}
				}

				buffer.position(buffer.limit());
				buffer.limit(buffer.capacity());

				log.debug("before next read=" + buffer);
			}
			return true;
		}

		void setCB(HTTPCB httpCB) {
			this.cb = httpCB;
			this.cancelledCB = new CB0() {
				protected void cb(CBResult result) {
					if (channel.isOpen()) {
						log.debug("HTTPCB cancelled. Closing connection to remoteAddr=" + remoteAddr);
						closeConnection(remoteAddr, channel);
					}
				}
			};
			httpCB.registerCancelledCB(cancelledCB);
		}
	}

	/*
	 * Callback to read request data from an existing connection
	 */
	enum RequestState {REQUEST, HEADERS, BODY};
	
	class ReadRequestConnHandler extends TCPComm.ReadConnHandler {
		protected final Log log = new Log(ReadRequestConnHandler.class);

		// Size of the buffer used for reading from connections
		private static final int UNKNOWN_LENGTH = -1;

		protected BufferedReader br = null;
		protected InputStream is = null;

		protected int bytesRead = 0;
		protected String httpVersion;
		protected int contentLength = UNKNOWN_LENGTH;
		protected String uri;
		protected String method;
		protected Map<String,String> headers;
		protected Map<String,String> parameters;
		
		//protected int dataStartMarker = 0;
		
		protected RequestState state = RequestState.REQUEST;
		
		protected boolean receivedEmptyLine = false;
		
		// Marker for the begining of the XML code
		protected int httpMarker = UNKNOWN_LENGTH;


		
		ReadRequestConnHandler(SelectableChannel channel, NetAddress remoteAddr) {
			super(channel, remoteAddr);
		}

		public Boolean cb(CBResult result, SelectionKey key) {
			super.cb(result, key);

			if (key.isReadable()) {
				log.debug("Reading from socket... remoteAddr=" + remoteAddr);
				channel = key.channel();
				final SocketChannel socketChannel = (SocketChannel) channel;

				if (buffer.remaining() == 0) {
					log.debug("The read buffer is full. buffer=" + buffer + ". Extending...");
					if (buffer.capacity() * 2 < MAX_BUFFER_SIZE) {
						buffer = PUtil.extendByteBuffer(buffer, buffer.capacity() * 2);
						buffer.limit(buffer.capacity());
						log.debug("new buffer=" + buffer);
					} else {
						log.warn("Buffer larger than MAX_BUFFER_SIZE: buffer=" + buffer + " remoteAddr="
								+ remoteAddr + " channel=" + channel);
						return false;
					}
				}

				int count = 0;
				try {
					count = socketChannel.read(buffer);
					// log.debug("count=" + count);
				} catch (IOException e) {
					log.error("Error reading from socket: " + e + " addr=" + remoteAddr);
				}

				final int finalCount = count;

				// Make sure that we only do a async call after having serviced the
				// socket
				AddressFactory.createResolved(socketChannel.socket().getInetAddress().getAddress(),
						socketChannel.socket().getPort(), new CB1<AddressIF>() {

							protected void cb(CBResult result, AddressIF resolvedAddr) {
								switch (result.state) {
									case OK: {
										int linecount = 0;
										remoteAddr = (NetAddress) resolvedAddr;

										if (finalCount == -1 || finalCount == 0) {
											/*
											 * There is some confusion on what count == 0 means but
											 * we'll just treat it as EOF and close the connection.
											 */
											log.debug("Connection to remoteAddr=" + remoteAddr + " has been closed.");
											deregister();
											closeConnection(remoteAddr, socketChannel);
											return;
										}

										buffer.limit(buffer.position());
										try {
											buffer.reset();
										} catch (InvalidMarkException e) {
											// No mark has been set yet
											buffer.position(0);
										}

										if (br == null) {
											is = PUtil.createInputStream(buffer);
											br = new BufferedReader(new InputStreamReader(is));
										}

										log.debug("buffer.position=" + buffer.position());
						
										// keep parsing until we get to the body
										if (state != RequestState.BODY) {
											
											String line = null;

											do {
												try {
													line = br.readLine();
													
												} catch (IOException e) {
													log.error("Failed reading line from buffer: " + e);
												}
												linecount++;
												log.debug ("line ("+linecount+")="+line);
												
												if (line != null) {
												
													bytesRead += line.length() + 2;
													
													switch (state) {
													case REQUEST:
														try {
															StringTokenizer tokenizer = new StringTokenizer(line);
															method = tokenizer.nextToken().toUpperCase();
															uri = tokenizer.nextToken();
															httpVersion = tokenizer.nextToken();
															parameters = getParameters(uri);
														} catch (NoSuchElementException e) {
															log.warn("Could not parse HTTP: " + e + " line=" + line);
															deregister();
															closeConnection(remoteAddr, socketChannel);
															return;
														}
														serverKeepAlive = serverKeepAlive && HTTP_11.equals(httpVersion);
														state = RequestState.HEADERS;
														break;
														
													case HEADERS:
														if (line.length() == 0) {
															state = RequestState.BODY;
															httpMarker = bytesRead;
															log.debug("httpMark=" + httpMarker);
															
														} else {
															String kv[] = line.split(": ");
															if (kv.length == 2) {
																if (headers == null) {
																	headers = new HashMap<String,String> ();
																}
																headers.put(kv[0], kv[1]);
																
																String key = kv[0].toLowerCase();
																String value = kv[1].toLowerCase();
																if (key.equals("content-length")) {
																	contentLength = Integer.parseInt(value);
																	log.debug("Content-length: " + contentLength);
																} else if (key.equals("connection:")) {
																	serverKeepAlive = serverKeepAlive
																			&& value.equals("keep-alive");
																}
																
															}
														}
														break;
													}

												}
											} while (line != null && line.length() != 0);

											log.debug("Null or empty line. contentLength=" + contentLength);

								
										}

										if (httpMarker != UNKNOWN_LENGTH) {
											log.debug("buffer.limit=" + buffer.limit());
											int availableData = buffer.limit() - httpMarker;
											if (availableData >= contentLength) {
												log.debug("Got the complete request.");
												buffer.position(httpMarker);

												ReadRequestConnHandler.this.deregister();

												// Note that this potentially includes more data than 
												// content-length said it would
												
												final String body = new String(buffer.array(), buffer.position(),
														buffer.remaining());
												EL.get().registerTimerCB(new CB0() {
													protected void cb(CBResult resultOK) {
														
														final HTTPCallbackHandler httpRequestHandler = getRequestHandler(uri);
														
														if (httpRequestHandler != null) {
															httpRequestHandler.call(CBResult.OK(), remoteAddr, method, uri,
																		headers, parameters, body,
																	new CB2<String, byte[]>(HTTP_HANDLER_TIMEOUT) {

																		protected void cb(CBResult result, String contentType,
																				byte[] httpResponse) {

																			WriteResponseConnHandler writeResponseConnHandler = getWriteResponseConnHandler(
																					channel, remoteAddr);

																			switch (result.state) {
																				case OK: {
																					writeResponseConnHandler.writeGoodHTTPResponse(
																							contentType, httpResponse, httpVersion,
																							serverKeepAlive);
																					break;
																				}
																				case TIMEOUT: {
																					log.warn("httpRequestHandler=" + httpRequestHandler
																							+ " :" + result.toString());
																					httpResponse = NetUtil
																							.toHTTPBytes("500 Internal Server Error - "
																									+ result.toString());
																					writeResponseConnHandler.writeBadHTTPResponse(
																							httpVersion, httpResponse, httpResponse);
																					break;
																				}
																				case ERROR: {
																					httpResponse = (httpResponse == null ? NetUtil
																							.toHTTPBytes("400 Bad Request - " + result.toString())
																							: httpResponse);
																					writeResponseConnHandler.writeBadHTTPResponse(
																							httpVersion, httpResponse, httpResponse);
																					break;
																				}
																			}
																			writeResponseConnHandler.register();
																		}
																	});
														} else {
															log.warn("Could not find a request handler for uri=" + uri + ". Ignoring.");
															
															byte[] httpResponse = NetUtil.toHTTPBytes("404 Not Found");
															
															WriteResponseConnHandler writeResponseConnHandler = getWriteResponseConnHandler(
																	channel, remoteAddr);

															writeResponseConnHandler.writeBadHTTPResponse(httpVersion, httpResponse, httpResponse);
															writeResponseConnHandler.register();
														}
													}
												});

												buffer.position(buffer.limit());

												// Discard the request from the buffer
												buffer.compact();

												try {
													br.close();
													is.close();
												} catch (IOException e) {
													log.error("Could not close inputstream. e=" + e);
												}

											} else {
												log.debug("Waiting for remainder of request. missing="
														+ (contentLength - availableData));
											}
										}

										// Revert the buffer to accept more data
										buffer.mark();
										buffer.position(buffer.limit());
										buffer.limit(buffer.capacity());
										break;
									}
									case TIMEOUT:
									case ERROR: {
										log.error(result.toString());
										break;
									}
								}
							}
						});
			}
			return true;
		}
	}

	/*
	 * Callback to write data from an existing connection
	 */
	class WriteResponseConnHandler extends TCPComm.WriteConnHandler {
		private final Log log = new Log(WriteResponseConnHandler.class);

		private boolean keepAlive = false;

		WriteResponseConnHandler(SelectableChannel channel, NetAddress remoteAddr) {
			super(channel, remoteAddr);
		}

		protected void writeGoodHTTPResponse(String contentType, byte[] httpResponse,
				String httpVersion, boolean keepAlive) {
			if (contentType.toLowerCase().contains("text")) {
				log.debug("httpResponse='" + httpResponse + "'");
			}
			this.keepAlive = keepAlive;

			buffer.put(NetUtil.toHTTPBytes(httpVersion));
			buffer.put(HTTP_OK);
			buffer.put(HTTP_SERVER);
			buffer.put(keepAlive ? HTTP_CONN_KEEP : HTTP_CONN_CLOSE);
			buffer.put(HTTP_CON_TYPE);
			buffer.put(NetUtil.toHTTPBytes(contentType));
			buffer.put(NetUtil.HTTP_NEWLINE);
			buffer.put(HTTP_CON_LENGTH);
			buffer.put(NetUtil.toHTTPBytes(Integer.toString(httpResponse.length)));
			buffer.put(NetUtil.HTTP_NEWLINE);
			buffer.put(NetUtil.HTTP_NEWLINE);

			/*
			 * TODO Make sure that the buffer grows here
			 */

			if (buffer.remaining() < httpResponse.length) {
				log.warn("Extending buffer=" + buffer + " response.length=" + httpResponse.length);
				buffer = PUtil.extendByteBuffer(buffer, buffer.capacity()
						+ (httpResponse.length - buffer.remaining()));
				buffer.limit(buffer.capacity());
				log.warn("new buffer=" + buffer);
			}

			buffer.put(httpResponse);
		}

		protected void writeBadHTTPResponse(String httpVersion, byte[] errorReason, byte[] errorData) {
			buffer.put(NetUtil.toHTTPBytes(httpVersion + " "));
			buffer.put(errorReason);
			buffer.put(NetUtil.HTTP_NEWLINE);
			buffer.put(HTTP_SERVER);
			buffer.put(NetUtil.HTTP_NEWLINE);
			buffer.put(errorData);
			// buffer.put(NetUtil.toHTTPBytes("Method " + httpMethod + " not
			// implemented (try POST)"));
		}

		public Boolean cb(CBResult result, SelectionKey key) {
			super.cb(result, key);
			SocketChannel socketChannel = (SocketChannel) channel;

			if (key.isWritable()) {
				buffer.flip();
				log.debug("Writing to socket... buffer=" + buffer);

				int count = 0;
				try {
					count = socketChannel.write(buffer);
				} catch (IOException e) {
					log.warn("Error writing to socket: " + e + " count=" + count + " buffer=" + buffer);
					closeConnection(remoteAddr, socketChannel);
					return true;
				}

				int limit = buffer.limit();
				log.debug("after write: count=" + count + " buffer=" + buffer);
				buffer.compact();
				buffer.position(limit - count);

				if (buffer.position() == 0) {
					log.debug("Write buffer is empty.");
					deregister();

					if (!keepAlive) {
						log.debug("No keepalive. Closing connection to remoteAddr=" + remoteAddr);
						closeConnection(remoteAddr, socketChannel);
					} else {
						ReadRequestConnHandler readRequestConnHandler = getReadRequestConnHandler(channel,
								remoteAddr);
						readRequestConnHandler.register();
					}
				}

			} else {
				log.error("We received a writable callback for a key that is not writable. Bug?");
			}
			return true;
		}
	}

	class HTTPRequest {

		AddressIF destAddr;
		URL url;
		String httpRequest;
		String contentType;
		boolean keepAlive;
		boolean streamSemantics;
		HTTPCB cb;

		HTTPRequest(AddressIF destAddr, URL url, String contentType, String httpRequest,
				boolean keepAlive, boolean streamSemantics, HTTPCB cb) {

			this.destAddr = destAddr;
			this.url = url;
			this.contentType = contentType;
			this.httpRequest = httpRequest;
			this.keepAlive = keepAlive;
			this.streamSemantics = streamSemantics;
			this.cb = cb;
		}
	}

	public static void main(String[] args) {
		EL.set(new EL());

		HTTPCommIF httpComm = new HTTPComm();

		final int timeout = 10000;

		httpComm.sendHTTPRequest("http://www.eecs.harvard.edu/index/eecs_index.php", null, false,
				new HTTPCB(timeout) {
					protected void cb(CBResult result, Integer resultCode, String httpResponse,
							String httpData) {
						switch (result.state) {
							case OK: {
								POut.p("resultCode=" + resultCode + " httpResponse=" + httpResponse + " httpData='"
										+ httpData + "'");
								break;
							}
							case TIMEOUT:
							case ERROR: {
								log.warn(result.toString());
								break;
							}
						}
					}
				});

		EL.get().exit();
		EL.get().main();

	}

}
