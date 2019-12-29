/*
 * @author Last modified by $Author: ledlie $
 * @version $Revision: 1.2 $ on $Date: 2009/03/27 17:39:58 $
 * @since Jan 9, 2005
 */
package edu.harvard.syrah.sbon.async.comm.xmlrpc;

import java.io.*;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.*;

import org.apache.xmlrpc.*;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.prp.NetUtil;
import edu.harvard.syrah.sbon.async.*;
import edu.harvard.syrah.sbon.async.CallbacksIF.*;
import edu.harvard.syrah.sbon.async.comm.AddressFactory;
import edu.harvard.syrah.sbon.async.comm.AddressIF;
import edu.harvard.syrah.sbon.async.comm.http.*;

/**
 * 
 * Implementation of the XMLRPC Communication module
 * 
 * TODO - add proper connection management with keepAlives (!!!) - test code
 * that handles queued requests
 * 
 */
public class XMLRPCComm extends HTTPComm implements XMLRPCCommIF {
	protected static final Log log = new Log(XMLRPCComm.class);

	private static final String HTTP_ENCODING = "US-ASCII";
	private static final String XMLRPC_PATH = "/xmlrpc";

	protected Map<String, Object> xmlRPCHandler = new HashMap<String, Object>();

	protected XMLRPCServers xmlRPCServers = new XMLRPCServers();

	// Constructor is protected
	protected XmlRpcRequestProcessor requestProcessor = new XmlRpcRequestProcessor() { /* empty */ };
	protected XmlRpcResponseProcessor responseProcessor = new XmlRpcResponseProcessor() { /* empty */	};

	public XMLRPCComm() {
		super();
	}

	public void initServer(AddressIF bindAddress, CB0 cbInit) {
		log.main("Binding to address=" + bindAddress + " with localAddress=" + localNetAddress);
		super.initServer(bindAddress, XmlRpc.getKeepAlive(), cbInit);
	}

	/*
	 * @see edu.harvard.syrah.sbon.comm.XMLRPCCommIF#call(java.lang.String,
	 *      java.lang.Object[])
	 */
	public void call(String urlString, String methodName, Object... args)
			throws MalformedURLException, UnknownHostException {
		call(urlString, methodName, null, args);
	}

	/*
	 * @see edu.harvard.syrah.sbon.comm.XMLRPCCommIF#call(java.lang.String,
	 *      java.util.Vector, edu.harvard.syrah.sbon.comm.XMLRPCCB)
	 */
	public void call(String urlString, String methodName, final XMLRPCCB cb, Object... args)
			throws MalformedURLException, UnknownHostException {

		log.debug("Calling XMLRPC method " + methodName + " with args=" + args + " and url="
				+ urlString);

		XmlRpcRequest XmlRpcRequest = new XmlRpcRequest(methodName, new Vector<Object>(
				Arrays.asList(args)));
		XmlRpcClientRequestProcessor xmlRpcClientRequestProcess = new XmlRpcClientRequestProcessor();

		byte[] request = null;
		try {
			request = xmlRpcClientRequestProcess.encodeRequestBytes(XmlRpcRequest, HTTP_ENCODING);
		} catch (XmlRpcClientException e) {
			log.error("Could not encode client request:" + e);
		}

		sendHTTPRequest(urlString, new String(request), XmlRpc.getKeepAlive(), new HTTPCB() {

			protected void cb(CBResult result, Integer resultCode, String httpResponse, String httpData) {
				switch (result.state) {
					case OK: {
						log.debug("Invoking XmlRpcWorker");
						// XmlRpc.debug = true;
						XmlRpcClientResponseProcessor xmlRpcClientResponseProcessor = new XmlRpcClientResponseProcessor();
						Object resultObject = null;
						InputStream is = new ByteArrayInputStream(NetUtil.toHTTPBytes(httpData));
						try {
							resultObject = xmlRpcClientResponseProcessor.decodeResponse(is);
						} catch (XmlRpcClientException e) {
							log.error("Could not decode result: " + e);
						}

						log.debug("Got a resultObject.");

						// do the callback
						if (!(resultObject instanceof XmlRpcException)) {
							cb.call(CBResult.OK(), resultObject);
						} else {
							cb.call(CBResult.ERROR("XmlRpcException"), resultObject);
						}

						break;
					}
					case TIMEOUT:
					case ERROR: {
						log.debug("Error calling xmlrpc method");
						cb.call(result, null);
						break;
					}
				}
			}
		});
	}

	/*
	 * @see edu.harvard.syrah.sbon.comm.XMLRPCCommIF#registerHandler(java.lang.String,
	 *      java.lang.Object)
	 */
	public void registerXMLRPCHandler(String handlerName, Object handler) {
		xmlRPCHandler.put(handlerName, new XMLRPCCallbackInvoker(handler));

		registerHandler(XMLRPC_PATH, new HTTPCallbackHandler() {
			
			protected void cb(CBResult result, AddressIF remoteAddr, String method, 
					String path, 
					Map<String,String> headers, Map<String,String> parameters,
					String httpRequest,
					final CB2<String, byte[]> cbHTTPResponse) {

				log.debug("Invoking XmlRpcWorker");
				// log.debug("buffer.remaining=" + buffer.remaining());
				// log.debug("buffer=" + Util.printChar(buffer));
				// XmlRpc.debug = true;

				InputStream is = new ByteArrayInputStream(NetUtil.toHTTPBytes(httpRequest));

				XmlRpcServerRequest request = requestProcessor.decodeRequest(is);
				
				// TODO take this from headers
				if (request == null) {
					byte[] error = responseProcessor.encodeException(new Exception(
							"No XMLRPC method invocation specified"), requestProcessor.getEncoding());
					cbHTTPResponse.call(CBResult.OK(), "text/xml", error);
					return;										
				}

				Object handler = null;
				try {
					handler = xmlRPCServers.getHandler(request.getMethodName());
				} catch (Exception e1) {					
					String errorString = "Could not get object handler for " + request.getMethodName() + ". e=" + e1; 
					byte[] error = responseProcessor.encodeException(new Exception(
							errorString), requestProcessor.getEncoding());
					cbHTTPResponse.call(CBResult.OK(), "text/xml", error);
					return;
				}

				if (!(handler instanceof XMLRPCCallbackHandlerIF))
					log.error("Can only invoke XMLRPCCallbackHandlers.");

				XMLRPCCallbackHandlerIF xmlRPCCallbackHandler = (XMLRPCCallbackHandlerIF) handler;
				log.debug("Calling xmlrpchandler...");
				try {
					xmlRPCCallbackHandler.execute(request.getMethodName(), request.getParameters(),
							new CB1<Object>() {

								public void cb(CBResult result, Object responseObject) {
									switch (result.state) {
										case OK: {
											log.debug("Received a response from server. responseObject=" + responseObject);
											byte[] response = null;
											try {
												response = responseProcessor.encodeResponse(responseObject,
														requestProcessor.getEncoding());
											} catch (UnsupportedEncodingException e) {
												log.error("Unsupported encoding: " + e);
											} catch (IOException e) {
												log.error("Could not encode response: " + e);
											} catch (XmlRpcException e) {
												log.error("Could not encode response: " + e);
											}
											cbHTTPResponse.call(result, "text/xml", response);
											break;
										}
										case TIMEOUT:
										case ERROR: {
											log.warn("Received an error response from server. " + result);
											byte[] error = responseProcessor.encodeException(new Exception(
													result.toString()), requestProcessor.getEncoding());
											cbHTTPResponse.call(CBResult.OK(), "text/xml", error);
											break;
										}
									}
								}
							});
				} catch (Exception e) {
					log.warn("Received an exception from server. e=" + e.toString());
					byte[] error = responseProcessor.encodeException(e, requestProcessor.getEncoding());
					log.debug("error.length=" + error.length);
					cbHTTPResponse.call(CBResult.OK(), "text/xml", error);
				}
			}
		});
	}

	public void deregisterXMLRPCHandler(String handlerName) {
		throw new UnsupportedOperationException();
	}

	class XMLRPCServers implements XmlRpcHandlerMapping {

		/*
		 * @see org.apache.xmlrpc.XmlRpcHandlerMapping#getHandler(java.lang.String)
		 */
		public Object getHandler(String arg0) throws Exception {
			Object handler = xmlRPCHandler.get(arg0.substring(0, arg0.indexOf('.')));
			if (handler == null)
				throw new Exception("Handler for " + arg0 + " not found.");

			return handler;
		}
	}

	public static void main(String[] args) {
		EventLoopIF eventLoop = new EL();

		EL.set(eventLoop);

		log.info("Testing the implementation of XMLRPCCommIF.");
		XMLRPCCommIF apiComm = new XMLRPCComm();
		int port = 16182;
		AddressIF apiAddress = AddressFactory.createLocalhost(port);

		apiComm.registerXMLRPCHandler("examples", new ExampleRPCHandler());
		apiComm.initServer(apiAddress, null);

		/*
		 * log.main("Making XMLRPC call..."); try {
		 * //apiComm.call("http://localhost:16181/", "examples.getStateName", new
		 * XMLRPCCB() {
		 * //apiComm.call("http://xmlrpc.usefulinc.com/demo/server.php",
		 * "system.listMethods", new XMLRPCCB() {
		 * apiComm.call("http://www.mirrorproject.com/xmlrpc/", "mirror.Random", new
		 * XMLRPCCB() {
		 * 
		 * public void cb(Object arg0) { log.info("Result of call is: " + arg0); }
		 * 
		 * }); } catch (MalformedURLException e1) { log.error("Wrong URL: " + e1); }
		 * catch (UnknownHostException e2) { log.error("Unknown host: " + e2); }
		 */

		log.main("Calling examples.getStateName");
		try {
			apiComm.call("http://localhost:16182/", "examples.getStateName", new XMLRPCCB() {

				public void cb(CBResult result, Object arg0) {
					log.info("Result of first call is: " + arg0.toString());
				}
			}, 42);
		} catch (MalformedURLException e1) {
			log.error("Wrong URL: " + e1);
		} catch (UnknownHostException e2) {
			log.error("Unknown host: " + e2);
		}

		/*
		 * log.main("Calling http://xmlrpc.usefulinc.com/demo/server.php
		 * system.listMethods"); try {
		 * apiComm.call("http://xmlrpc.usefulinc.com/demo/server.php",
		 * "system.listMethods", new XMLRPCCB() { public void cb(Object arg0) {
		 * log.info("Result of second call is: " + arg0.toString()); } }); } catch
		 * (MalformedURLException e1) { log.error("Wrong URL: " + e1); } catch
		 * (UnknownHostException e2) { log.error("Unknown host: " + e2); }
		 */

		EL.get().registerTimerCB(10000, null);
		EL.get().exit();
		EL.get().main();
	}
}
